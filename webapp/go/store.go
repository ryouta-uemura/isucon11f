package main

import (
	"database/sql"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

// ---------------------------------------------------------------------
// 実験版: 全データをオンメモリに保持する。
//
// 意図的に永続化(DB書き込み)を一切行わない。/initialize でMySQLへ
// 初期データを流し込んだ直後に一度だけ全件読み込み、以降の読み書きは
// このプロセス内メモリだけで完結する。再起動でデータは消える
// (再起動試験には非対応)。効果測定専用のバージョンであることに注意。
//
// ロック方針:
//   - Store 直下のトップレベルmap(courses/classes/announcements)は
//     「新規追加(構造変更)」だけを短時間ロックする専用mutexで守る。
//     中身(Status等)の読み書きは各エンティティ自身が持つロックに任せる。
//   - users / userStates は /initialize 後は増減しない(実行時にユーザー
//     が増えることはない)ので、トップレベルmapはロック不要。
//   - Course.Status / Class.SubmissionClosed は書き込みが単発かつ
//     読み取りが非常に高頻度なので、mutexではなくatomic int32で持つ。
//   - 課題提出(SubmitAssignment, 得点×3)は Class 単位のロックのみで完結し、
//     他の科目・他の講義とは一切competeしない。
//   - お知らせ(GetAnnouncementList, 得点×1)は UserState 単位のロックのみで
//     完結し、他の学生とは一切competeしない。
// ---------------------------------------------------------------------

const (
	courseStatusRegistration int32 = iota
	courseStatusInProgress
	courseStatusClosed
)

func courseStatusToCode(s CourseStatus) int32 {
	switch s {
	case StatusInProgress:
		return courseStatusInProgress
	case StatusClosed:
		return courseStatusClosed
	default:
		return courseStatusRegistration
	}
}

func courseStatusFromCode(v int32) CourseStatus {
	switch v {
	case courseStatusInProgress:
		return StatusInProgress
	case courseStatusClosed:
		return StatusClosed
	default:
		return StatusRegistration
	}
}

type Submission struct {
	UserID   string
	FileName string
	Data     []byte
	Score    *int
}

// Course はコード/名称/教員などの不変フィールドと、可変な状態
// (ステータス・講義一覧・履修者集合)を分けて持つ。
// 可変部分は Course 自身が持つロックでのみ保護され、他の科目には一切影響しない。
type Course struct {
	ID          string
	Code        string
	Type        CourseType
	Name        string
	Description string
	Credit      uint8
	Period      uint8
	DayOfWeek   DayOfWeek
	TeacherID   string
	Keywords    string

	statusCode int32 // atomic

	classesMu   sync.RWMutex
	classes     []*Class // part昇順
	classByPart map[uint8]*Class

	usersMu   sync.RWMutex
	users     map[string]bool // 履修者(student)のuserID集合
}

func NewCourse(id, code string, typ CourseType, name, description string, credit, period uint8, dayOfWeek DayOfWeek, teacherID, keywords string, status CourseStatus) *Course {
	return &Course{
		ID:          id,
		Code:        code,
		Type:        typ,
		Name:        name,
		Description: description,
		Credit:      credit,
		Period:      period,
		DayOfWeek:   dayOfWeek,
		TeacherID:   teacherID,
		Keywords:    keywords,
		statusCode:  courseStatusToCode(status),
		classByPart: map[uint8]*Class{},
		users:       map[string]bool{},
	}
}

func (c *Course) Status() CourseStatus {
	return courseStatusFromCode(atomic.LoadInt32(&c.statusCode))
}

func (c *Course) SetStatus(s CourseStatus) {
	atomic.StoreInt32(&c.statusCode, courseStatusToCode(s))
}

func (c *Course) AddRegisteredUser(userID string) {
	c.usersMu.Lock()
	c.users[userID] = true
	c.usersMu.Unlock()
}

func (c *Course) HasRegisteredUser(userID string) bool {
	c.usersMu.RLock()
	defer c.usersMu.RUnlock()
	return c.users[userID]
}

// RegisteredUserIDs はスナップショットを返す。返した後にロックは持たない。
func (c *Course) RegisteredUserIDs() []string {
	c.usersMu.RLock()
	defer c.usersMu.RUnlock()
	ids := make([]string, 0, len(c.users))
	for uid := range c.users {
		ids = append(ids, uid)
	}
	return ids
}

// Classes は part 昇順のスナップショットを返す。
func (c *Course) Classes() []*Class {
	c.classesMu.RLock()
	defer c.classesMu.RUnlock()
	out := make([]*Class, len(c.classes))
	copy(out, c.classes)
	return out
}

// AddClassIfAbsent は (part) の重複時は既存の *Class を返す(第2戻り値false)。
func (c *Course) AddClassIfAbsent(id string, part uint8, title, description string) (*Class, bool) {
	c.classesMu.Lock()
	defer c.classesMu.Unlock()
	if existing, ok := c.classByPart[part]; ok {
		return existing, false
	}
	class := NewClass(id, c.ID, part, title, description)
	c.classes = append(c.classes, class)
	c.classByPart[part] = class
	return class, true
}

// Class は不変フィールドと、可変な提出物集合を分けて持つ。
// SubmissionClosed はatomicで持ち、提出物のmapは専用ロックで守る。
// このロックは講義(=classID)単位なので、他の講義の提出とは一切競合しない。
type Class struct {
	ID          string
	CourseID    string
	Part        uint8
	Title       string
	Description string

	closed int32 // atomic (0/1)

	subMu           sync.RWMutex
	submissions     map[string]*Submission // userID -> submission
	submissionOrder []string                // 追加順のuserID一覧(zip生成用)
}

func NewClass(id, courseID string, part uint8, title, description string) *Class {
	return &Class{
		ID:          id,
		CourseID:    courseID,
		Part:        part,
		Title:       title,
		Description: description,
		submissions: map[string]*Submission{},
	}
}

func (cl *Class) Closed() bool {
	return atomic.LoadInt32(&cl.closed) != 0
}

func (cl *Class) Close() {
	atomic.StoreInt32(&cl.closed, 1)
}

// GetSubmission は自分の提出状況を確認するためのもの。呼び出し元は
// 返却された *Submission のフィールドを直接書き換えてはいけない
// (Score の更新は SetScore を使う)。
func (cl *Class) GetSubmission(userID string) (*Submission, bool) {
	cl.subMu.RLock()
	defer cl.subMu.RUnlock()
	sub, ok := cl.submissions[userID]
	return sub, ok
}

func (cl *Class) SubmissionCount() int {
	cl.subMu.RLock()
	defer cl.subMu.RUnlock()
	return len(cl.submissions)
}

// Upsert は新規提出なら追加してtrueを、再提出(ファイル差し替え)ならfalseを返す。
func (cl *Class) UpsertSubmission(userID, fileName string, data []byte) {
	cl.subMu.Lock()
	defer cl.subMu.Unlock()
	if existing, ok := cl.submissions[userID]; ok {
		existing.FileName = fileName
		existing.Data = data
		return
	}
	cl.submissions[userID] = &Submission{FileName: fileName, Data: data}
	cl.submissionOrder = append(cl.submissionOrder, userID)
}

// SetScore は該当ユーザーの提出が存在する場合のみ点数を設定する。
func (cl *Class) SetScore(userID string, score int) {
	cl.subMu.Lock()
	defer cl.subMu.Unlock()
	if sub, ok := cl.submissions[userID]; ok {
		v := score
		sub.Score = &v
	}
}

type submissionEntry struct {
	UserID   string
	FileName string
	Data     []byte
}

// AllSubmissions は追加順のスナップショットを返す(zip生成用)。
func (cl *Class) AllSubmissions() []submissionEntry {
	cl.subMu.RLock()
	defer cl.subMu.RUnlock()
	out := make([]submissionEntry, 0, len(cl.submissionOrder))
	for _, uid := range cl.submissionOrder {
		sub := cl.submissions[uid]
		out = append(out, submissionEntry{UserID: uid, FileName: sub.FileName, Data: sub.Data})
	}
	return out
}

type Announcement struct {
	ID         string
	CourseID   string
	CourseName string
	Title      string
	Message    string
}

type UserAnnouncementState struct {
	AnnouncementID string
	unread         int32 // atomic (0/1)
}

func (u *UserAnnouncementState) Unread() bool {
	return atomic.LoadInt32(&u.unread) != 0
}

// MarkRead は初回既読化ならtrue、既に既読済みならfalseを返す。
func (u *UserAnnouncementState) MarkRead() bool {
	return atomic.CompareAndSwapInt32(&u.unread, 1, 0)
}

// UserState は学生1人分の可変状態(履修中の科目集合・お知らせの既読状態)を
// まとめて1本のロックで守る。学生ごとに独立しているので、
// 他の学生の操作とは一切競合しない。
type UserState struct {
	mu sync.RWMutex

	registrations map[string]bool // courseID集合

	announcements     []*UserAnnouncementState // 作成順(昇順)
	announcementIndex map[string]*UserAnnouncementState
	unreadCount       int32 // atomic
}

func NewUserState() *UserState {
	return &UserState{
		registrations:     map[string]bool{},
		announcementIndex: map[string]*UserAnnouncementState{},
	}
}

func (u *UserState) IsRegistered(courseID string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.registrations[courseID]
}

func (u *UserState) RegisteredCourseIDs() []string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	ids := make([]string, 0, len(u.registrations))
	for cid := range u.registrations {
		ids = append(ids, cid)
	}
	return ids
}

func (u *UserState) Register(courseID string) {
	u.mu.Lock()
	u.registrations[courseID] = true
	u.mu.Unlock()
}

func (u *UserState) AddAnnouncement(announcementID string, unread bool) {
	ua := &UserAnnouncementState{AnnouncementID: announcementID}
	if unread {
		ua.unread = 1
	}
	u.mu.Lock()
	u.announcements = append(u.announcements, ua)
	u.announcementIndex[announcementID] = ua
	if unread {
		atomic.AddInt32(&u.unreadCount, 1)
	}
	u.mu.Unlock()
}

func (u *UserState) UnreadCount() int {
	return int(atomic.LoadInt32(&u.unreadCount))
}

// MarkAnnouncementRead は既読化前の状態(true=未読だった)を返す。
// 対象が存在しなければ ok=false。
func (u *UserState) MarkAnnouncementRead(announcementID string) (wasUnread bool, ok bool) {
	u.mu.RLock()
	ua, exists := u.announcementIndex[announcementID]
	u.mu.RUnlock()
	if !exists {
		return false, false
	}
	if ua.MarkRead() {
		atomic.AddInt32(&u.unreadCount, -1)
		return true, true
	}
	return false, true
}

// AnnouncementsDesc は AnnouncementID(ULID = 生成順と一致する文字列)の
// 降順(新しい順)で、courseFilterが空でなければ該当科目のものだけに
// 絞ったスナップショットを返す。
//
// 注意: u.announcements への追加順(=このユーザーへブロードキャストが
// 届いた順)は、生成順(=ID順)と一致しない。複数科目の教員goroutineが
// 並行して AddAnnouncement を叩くため、IDが小さい(先に生成された)
// お知らせが、IDが大きい(後で生成された)別科目のお知らせより後に
// このユーザーの一覧へ届くことがある。元のSQLは `ORDER BY id DESC` で
// 到着順ではなくID自体でソートしていたので、ここでも明示的にIDでソートする。
func (u *UserState) AnnouncementsDesc(st *Store, courseFilter string) []*UserAnnouncementState {
	u.mu.RLock()
	snapshot := make([]*UserAnnouncementState, len(u.announcements))
	copy(snapshot, u.announcements)
	u.mu.RUnlock()

	out := make([]*UserAnnouncementState, 0, len(snapshot))
	for _, ua := range snapshot {
		if courseFilter != "" {
			ann, ok := st.GetAnnouncement(ua.AnnouncementID)
			if !ok || ann.CourseID != courseFilter {
				continue
			}
		}
		out = append(out, ua)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AnnouncementID > out[j].AnnouncementID
	})
	return out
}

// Store はプロセス全体で共有される唯一のオンメモリDB。
type Store struct {
	// users / userStates は /initialize 完了後は構造(キー集合)が変化しない
	// ため、map自体へのロックは不要(値であるUserStateの中身は各自のmuで保護)。
	usersByID   map[string]*User
	usersByCode map[string]*User
	userStates  map[string]*UserState // userID -> state

	coursesMu     sync.RWMutex // courses / coursesByCode への「追加」だけを守る
	courses       map[string]*Course
	coursesByCode map[string]*Course

	classesMu sync.RWMutex // classes への「追加」だけを守る(classID -> *Class の直引き用)
	classes   map[string]*Class

	announcementsMu sync.RWMutex // announcements への「追加」だけを守る
	announcements   map[string]*Announcement
}

func NewStore() *Store {
	return &Store{
		usersByID:     map[string]*User{},
		usersByCode:   map[string]*User{},
		userStates:    map[string]*UserState{},
		courses:       map[string]*Course{},
		coursesByCode: map[string]*Course{},
		classes:       map[string]*Class{},
		announcements: map[string]*Announcement{},
	}
}

func (st *Store) GetUserState(userID string) *UserState {
	// userStates は /initialize 後に構造変化しないため無ロックで読める。
	return st.userStates[userID]
}

func (st *Store) GetCourse(id string) (*Course, bool) {
	st.coursesMu.RLock()
	defer st.coursesMu.RUnlock()
	c, ok := st.courses[id]
	return c, ok
}

func (st *Store) GetCourseByCode(code string) (*Course, bool) {
	st.coursesMu.RLock()
	defer st.coursesMu.RUnlock()
	c, ok := st.coursesByCode[code]
	return c, ok
}

// AllCourses は code 昇順のスナップショットを返す。
func (st *Store) AllCourses() []*Course {
	st.coursesMu.RLock()
	list := make([]*Course, 0, len(st.courses))
	for _, c := range st.courses {
		list = append(list, c)
	}
	st.coursesMu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return list[i].Code < list[j].Code })
	return list
}

// AddCourseIfAbsent は code の重複時は既存の *Course を返す(第2戻り値false)。
func (st *Store) AddCourseIfAbsent(course *Course) (*Course, bool) {
	st.coursesMu.Lock()
	defer st.coursesMu.Unlock()
	if existing, ok := st.coursesByCode[course.Code]; ok {
		return existing, false
	}
	st.courses[course.ID] = course
	st.coursesByCode[course.Code] = course
	return course, true
}

func (st *Store) registerCourseStructure(course *Course) {
	st.coursesMu.Lock()
	st.courses[course.ID] = course
	st.coursesByCode[course.Code] = course
	st.coursesMu.Unlock()
}

func (st *Store) GetClass(id string) (*Class, bool) {
	st.classesMu.RLock()
	defer st.classesMu.RUnlock()
	c, ok := st.classes[id]
	return c, ok
}

func (st *Store) registerClassStructure(class *Class) {
	st.classesMu.Lock()
	st.classes[class.ID] = class
	st.classesMu.Unlock()
}

func (st *Store) GetAnnouncement(id string) (*Announcement, bool) {
	st.announcementsMu.RLock()
	defer st.announcementsMu.RUnlock()
	a, ok := st.announcements[id]
	return a, ok
}

// AddAnnouncementIfAbsent は id の重複時は既存の *Announcement を返す(第2戻り値false)。
func (st *Store) AddAnnouncementIfAbsent(ann *Announcement) (*Announcement, bool) {
	st.announcementsMu.Lock()
	defer st.announcementsMu.Unlock()
	if existing, ok := st.announcements[ann.ID]; ok {
		return existing, false
	}
	st.announcements[ann.ID] = ann
	return ann, true
}

// LoadStoreFromDB は /initialize 直後に一度だけ呼ばれ、MySQL上の全データを
// メモリへ展開する。以降このプロセスはリクエスト処理中にDBへ触れない。
func LoadStoreFromDB(db *sqlx.DB) (*Store, error) {
	st := NewStore()

	var users []User
	if err := db.Select(&users, "SELECT * FROM `users`"); err != nil {
		return nil, err
	}
	for i := range users {
		u := &users[i]
		st.usersByID[u.ID] = u
		st.usersByCode[u.Code] = u
		st.userStates[u.ID] = NewUserState()
	}

	type courseDBRow struct {
		ID          string       `db:"id"`
		Code        string       `db:"code"`
		Type        CourseType   `db:"type"`
		Name        string       `db:"name"`
		Description string       `db:"description"`
		Credit      uint8        `db:"credit"`
		Period      uint8        `db:"period"`
		DayOfWeek   DayOfWeek    `db:"day_of_week"`
		TeacherID   string       `db:"teacher_id"`
		Keywords    string       `db:"keywords"`
		Status      CourseStatus `db:"status"`
	}
	var courseRows []courseDBRow
	if err := db.Select(&courseRows, "SELECT * FROM `courses`"); err != nil {
		return nil, err
	}
	for i := range courseRows {
		cr := &courseRows[i]
		course := NewCourse(cr.ID, cr.Code, cr.Type, cr.Name, cr.Description, cr.Credit, cr.Period, cr.DayOfWeek, cr.TeacherID, cr.Keywords, cr.Status)
		st.registerCourseStructure(course)
	}

	type classDBRow struct {
		ID               string `db:"id"`
		CourseID         string `db:"course_id"`
		Part             uint8  `db:"part"`
		Title            string `db:"title"`
		Description      string `db:"description"`
		SubmissionClosed bool   `db:"submission_closed"`
	}
	var classRows []classDBRow
	if err := db.Select(&classRows, "SELECT * FROM `classes` ORDER BY `part` ASC"); err != nil {
		return nil, err
	}
	for i := range classRows {
		clr := &classRows[i]
		course, ok := st.GetCourse(clr.CourseID)
		if !ok {
			continue
		}
		class, created := course.AddClassIfAbsent(clr.ID, clr.Part, clr.Title, clr.Description)
		if created {
			if clr.SubmissionClosed {
				class.Close()
			}
			st.registerClassStructure(class)
		}
	}

	type regRow struct {
		CourseID string `db:"course_id"`
		UserID   string `db:"user_id"`
	}
	var regs []regRow
	if err := db.Select(&regs, "SELECT `course_id`, `user_id` FROM `registrations`"); err != nil {
		return nil, err
	}
	for _, r := range regs {
		if us := st.GetUserState(r.UserID); us != nil {
			us.Register(r.CourseID)
		}
		if course, ok := st.GetCourse(r.CourseID); ok {
			course.AddRegisteredUser(r.UserID)
		}
	}

	type subRow struct {
		UserID   string        `db:"user_id"`
		ClassID  string        `db:"class_id"`
		FileName string        `db:"file_name"`
		Score    sql.NullInt64 `db:"score"`
	}
	var subs []subRow
	if err := db.Select(&subs, "SELECT `user_id`, `class_id`, `file_name`, `score` FROM `submissions`"); err != nil {
		return nil, err
	}
	for _, s := range subs {
		class, ok := st.GetClass(s.ClassID)
		if !ok {
			continue
		}
		// 初期データの提出物本体は webapp/data/ 配下に
		// "<classID>-<userID>.pdf" という名前で置かれている
		data, err := os.ReadFile(InitDataDirectory + s.ClassID + "-" + s.UserID + ".pdf")
		if err != nil {
			data = nil
		}
		class.UpsertSubmission(s.UserID, s.FileName, data)
		if s.Score.Valid {
			class.SetScore(s.UserID, int(s.Score.Int64))
		}
	}

	type annRow struct {
		ID       string `db:"id"`
		CourseID string `db:"course_id"`
		Title    string `db:"title"`
		Message  string `db:"message"`
	}
	var anns []annRow
	if err := db.Select(&anns, "SELECT `id`, `course_id`, `title`, `message` FROM `announcements`"); err != nil {
		return nil, err
	}
	for _, a := range anns {
		courseName := ""
		if course, ok := st.GetCourse(a.CourseID); ok {
			courseName = course.Name
		}
		ann := &Announcement{ID: a.ID, CourseID: a.CourseID, CourseName: courseName, Title: a.Title, Message: a.Message}
		st.AddAnnouncementIfAbsent(ann)
	}

	type unreadRow struct {
		AnnouncementID string `db:"announcement_id"`
		UserID         string `db:"user_id"`
		IsDeleted      bool   `db:"is_deleted"`
	}
	var unreads []unreadRow
	// announcement_id (ULID) 昇順 = 作成順。ページングの並び順を再現するために必要。
	if err := db.Select(&unreads, "SELECT `announcement_id`, `user_id`, `is_deleted` FROM `unread_announcements` ORDER BY `announcement_id` ASC"); err != nil {
		return nil, err
	}
	for _, u := range unreads {
		us := st.GetUserState(u.UserID)
		if us == nil {
			continue
		}
		us.AddAnnouncement(u.AnnouncementID, !u.IsDeleted)
	}

	return st, nil
}
