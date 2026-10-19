package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/crypto/bcrypt"
)

const (
	SQLDirectory      = "../sql/"
	InitDataDirectory = "../data/"
	SessionName       = "isucholar_go"
)

type handlers struct {
	DB *sqlx.DB

	storeMu sync.RWMutex
	store   *Store
}

func (h *handlers) getStore() *Store {
	h.storeMu.RLock()
	defer h.storeMu.RUnlock()
	return h.store
}

func (h *handlers) setStore(s *Store) {
	h.storeMu.Lock()
	defer h.storeMu.Unlock()
	h.store = s
}

func main() {
	e := echo.New()
	e.Debug = GetEnv("DEBUG", "") == "true"
	e.Server.Addr = fmt.Sprintf(":%v", GetEnv("PORT", "7000"))
	e.HideBanner = true

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(session.Middleware(sessions.NewCookieStore([]byte("trapnomura"))))

	db, _ := GetDB(false)
	db.SetMaxOpenConns(10)

	h := &handlers{
		DB:    db,
		store: NewStore(),
	}

	e.POST("/initialize", h.Initialize)

	e.POST("/login", h.Login)
	e.POST("/logout", h.Logout)
	API := e.Group("/api", h.IsLoggedIn)
	{
		usersAPI := API.Group("/users")
		{
			usersAPI.GET("/me", h.GetMe)
			usersAPI.GET("/me/courses", h.GetRegisteredCourses)
			usersAPI.PUT("/me/courses", h.RegisterCourses)
			usersAPI.GET("/me/grades", h.GetGrades)
		}
		coursesAPI := API.Group("/courses")
		{
			coursesAPI.GET("", h.SearchCourses)
			coursesAPI.POST("", h.AddCourse, h.IsAdmin)
			coursesAPI.GET("/:courseID", h.GetCourseDetail)
			coursesAPI.PUT("/:courseID/status", h.SetCourseStatus, h.IsAdmin)
			coursesAPI.GET("/:courseID/classes", h.GetClasses)
			coursesAPI.POST("/:courseID/classes", h.AddClass, h.IsAdmin)
			coursesAPI.POST("/:courseID/classes/:classID/assignments", h.SubmitAssignment)
			coursesAPI.PUT("/:courseID/classes/:classID/assignments/scores", h.RegisterScores, h.IsAdmin)
			coursesAPI.GET("/:courseID/classes/:classID/assignments/export", h.DownloadSubmittedAssignments, h.IsAdmin)
		}
		announcementsAPI := API.Group("/announcements")
		{
			announcementsAPI.GET("", h.GetAnnouncementList)
			announcementsAPI.POST("", h.AddAnnouncement, h.IsAdmin)
			announcementsAPI.GET("/:announcementID", h.GetAnnouncementDetail)
		}
	}

	e.Logger.Error(e.StartServer(e.Server))
}

type InitializeResponse struct {
	Language string `json:"language"`
}

// Initialize POST /initialize 初期化エンドポイント
//
// 実験版: MySQLへスキーマ+初期データを流し込むところまでは元実装と同じ。
// そのあと全件をメモリへロードし、以降のリクエストはこのメモリのみで処理する
// (このプロセスが生きている間はDBへ二度と触らない)。
func (h *handlers) Initialize(c echo.Context) error {
	dbForInit, err := GetDB(true)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer dbForInit.Close()

	files := []string{
		"1_schema.sql",
		"2_init.sql",
		"3_sample.sql",
	}
	for _, file := range files {
		data, err := os.ReadFile(SQLDirectory + file)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		if _, err := dbForInit.Exec(string(data)); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	newStore, err := LoadStoreFromDB(dbForInit)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	h.setStore(newStore)

	res := InitializeResponse{
		Language: "go",
	}
	return c.JSON(http.StatusOK, res)
}

// IsLoggedIn ログイン確認用middleware
func (h *handlers) IsLoggedIn(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		sess, err := session.Get(SessionName, c)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		if sess.IsNew {
			return c.String(http.StatusUnauthorized, "You are not logged in.")
		}
		_, ok := sess.Values["userID"]
		if !ok {
			return c.String(http.StatusUnauthorized, "You are not logged in.")
		}

		return next(c)
	}
}

// IsAdmin admin確認用middleware
func (h *handlers) IsAdmin(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		sess, err := session.Get(SessionName, c)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		isAdmin, ok := sess.Values["isAdmin"]
		if !ok {
			c.Logger().Error("failed to get isAdmin from session")
			return c.NoContent(http.StatusInternalServerError)
		}
		if !isAdmin.(bool) {
			return c.String(http.StatusForbidden, "You are not admin user.")
		}

		return next(c)
	}
}

func getUserInfo(c echo.Context) (userID string, userName string, isAdmin bool, err error) {
	sess, err := session.Get(SessionName, c)
	if err != nil {
		return "", "", false, err
	}
	_userID, ok := sess.Values["userID"]
	if !ok {
		return "", "", false, errors.New("failed to get userID from session")
	}
	_userName, ok := sess.Values["userName"]
	if !ok {
		return "", "", false, errors.New("failed to get userName from session")
	}
	_isAdmin, ok := sess.Values["isAdmin"]
	if !ok {
		return "", "", false, errors.New("failed to get isAdmin from session")
	}
	return _userID.(string), _userName.(string), _isAdmin.(bool), nil
}

type UserType string

const (
	Student UserType = "student"
	Teacher UserType = "teacher"
)

type User struct {
	ID             string   `db:"id"`
	Code           string   `db:"code"`
	Name           string   `db:"name"`
	HashedPassword []byte   `db:"hashed_password"`
	Type           UserType `db:"type"`
}

type CourseType string

const (
	LiberalArts   CourseType = "liberal-arts"
	MajorSubjects CourseType = "major-subjects"
)

type DayOfWeek string

const (
	Monday    DayOfWeek = "monday"
	Tuesday   DayOfWeek = "tuesday"
	Wednesday DayOfWeek = "wednesday"
	Thursday  DayOfWeek = "thursday"
	Friday    DayOfWeek = "friday"
)

var daysOfWeek = []DayOfWeek{Monday, Tuesday, Wednesday, Thursday, Friday}

type CourseStatus string

const (
	StatusRegistration CourseStatus = "registration"
	StatusInProgress   CourseStatus = "in-progress"
	StatusClosed       CourseStatus = "closed"
)

// ---------- Public API ----------

type LoginRequest struct {
	Code     string `json:"code"`
	Password string `json:"password"`
}

// Login POST /login ログイン
func (h *handlers) Login(c echo.Context) error {
	var req LoginRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	st := h.getStore()
	user, ok := st.usersByCode[req.Code]
	if !ok {
		return c.String(http.StatusUnauthorized, "Code or Password is wrong.")
	}

	if bcrypt.CompareHashAndPassword(user.HashedPassword, []byte(req.Password)) != nil {
		return c.String(http.StatusUnauthorized, "Code or Password is wrong.")
	}

	sess, err := session.Get(SessionName, c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if userID, ok := sess.Values["userID"].(string); ok && userID == user.ID {
		return c.String(http.StatusBadRequest, "You are already logged in.")
	}

	sess.Values["userID"] = user.ID
	sess.Values["userName"] = user.Name
	sess.Values["isAdmin"] = user.Type == Teacher
	sess.Options = &sessions.Options{
		Path:   "/",
		MaxAge: 3600,
	}

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}

// Logout POST /logout ログアウト
func (h *handlers) Logout(c echo.Context) error {
	sess, err := session.Get(SessionName, c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	sess.Options = &sessions.Options{
		Path:   "/",
		MaxAge: -1,
	}

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}

// ---------- Users API ----------

type GetMeResponse struct {
	Code    string `json:"code"`
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

// GetMe GET /api/users/me 自身の情報を取得
func (h *handlers) GetMe(c echo.Context) error {
	userID, userName, isAdmin, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	st := h.getStore()
	user, ok := st.usersByID[userID]
	if !ok {
		c.Logger().Error("unknown userID in session")
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, GetMeResponse{
		Code:    user.Code,
		Name:    userName,
		IsAdmin: isAdmin,
	})
}

type GetRegisteredCourseResponseContent struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Teacher   string    `json:"teacher"`
	Period    uint8     `json:"period"`
	DayOfWeek DayOfWeek `json:"day_of_week"`
}

// GetRegisteredCourses GET /api/users/me/courses 履修中の科目一覧取得
func (h *handlers) GetRegisteredCourses(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	st := h.getStore()
	us := st.GetUserState(userID)

	res := make([]GetRegisteredCourseResponseContent, 0)
	for _, courseID := range us.RegisteredCourseIDs() {
		course, ok := st.GetCourse(courseID)
		if !ok || course.Status() == StatusClosed {
			continue
		}
		teacher := st.usersByID[course.TeacherID]
		res = append(res, GetRegisteredCourseResponseContent{
			ID:        course.ID,
			Name:      course.Name,
			Teacher:   teacher.Name,
			Period:    course.Period,
			DayOfWeek: course.DayOfWeek,
		})
	}

	return c.JSON(http.StatusOK, res)
}

type RegisterCourseRequestContent struct {
	ID string `json:"id"`
}

type RegisterCoursesErrorResponse struct {
	CourseNotFound       []string `json:"course_not_found,omitempty"`
	NotRegistrableStatus []string `json:"not_registrable_status,omitempty"`
	ScheduleConflict     []string `json:"schedule_conflict,omitempty"`
}

// RegisterCourses PUT /api/users/me/courses 履修登録
func (h *handlers) RegisterCourses(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var req []RegisterCourseRequestContent
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	sort.Slice(req, func(i, j int) bool {
		return req[i].ID < req[j].ID
	})

	st := h.getStore()
	us := st.GetUserState(userID)

	var errs RegisterCoursesErrorResponse
	var newlyAdded []*Course
	for _, courseReq := range req {
		course, ok := st.GetCourse(courseReq.ID)
		if !ok {
			errs.CourseNotFound = append(errs.CourseNotFound, courseReq.ID)
			continue
		}

		if course.Status() != StatusRegistration {
			errs.NotRegistrableStatus = append(errs.NotRegistrableStatus, course.ID)
			continue
		}

		// すでに履修登録済みの科目は無視する
		if us.IsRegistered(course.ID) {
			continue
		}

		newlyAdded = append(newlyAdded, course)
	}

	var alreadyRegistered []*Course
	for _, courseID := range us.RegisteredCourseIDs() {
		course, ok := st.GetCourse(courseID)
		if ok && course.Status() != StatusClosed {
			alreadyRegistered = append(alreadyRegistered, course)
		}
	}
	alreadyRegistered = append(alreadyRegistered, newlyAdded...)

	for _, course1 := range newlyAdded {
		for _, course2 := range alreadyRegistered {
			if course1.ID != course2.ID && course1.Period == course2.Period && course1.DayOfWeek == course2.DayOfWeek {
				errs.ScheduleConflict = append(errs.ScheduleConflict, course1.ID)
				break
			}
		}
	}

	if len(errs.CourseNotFound) > 0 || len(errs.NotRegistrableStatus) > 0 || len(errs.ScheduleConflict) > 0 {
		return c.JSON(http.StatusBadRequest, errs)
	}

	for _, course := range newlyAdded {
		us.Register(course.ID)
		course.AddRegisteredUser(userID)
	}

	return c.NoContent(http.StatusOK)
}

type GetGradeResponse struct {
	Summary       Summary        `json:"summary"`
	CourseResults []CourseResult `json:"courses"`
}

type Summary struct {
	Credits   int     `json:"credits"`
	GPA       float64 `json:"gpa"`
	GpaTScore float64 `json:"gpa_t_score"` // 偏差値
	GpaAvg    float64 `json:"gpa_avg"`     // 平均値
	GpaMax    float64 `json:"gpa_max"`     // 最大値
	GpaMin    float64 `json:"gpa_min"`     // 最小値
}

type CourseResult struct {
	Name             string       `json:"name"`
	Code             string       `json:"code"`
	TotalScore       int          `json:"total_score"`
	TotalScoreTScore float64      `json:"total_score_t_score"` // 偏差値
	TotalScoreAvg    float64      `json:"total_score_avg"`     // 平均値
	TotalScoreMax    int          `json:"total_score_max"`     // 最大値
	TotalScoreMin    int          `json:"total_score_min"`     // 最小値
	ClassScores      []ClassScore `json:"class_scores"`
}

type ClassScore struct {
	ClassID    string `json:"class_id"`
	Title      string `json:"title"`
	Part       uint8  `json:"part"`
	Score      *int   `json:"score"`      // 0~100点
	Submitters int    `json:"submitters"` // 提出した学生数
}

// GetGrades GET /api/users/me/grades 成績取得
func (h *handlers) GetGrades(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	st := h.getStore()
	us := st.GetUserState(userID)
	registeredCourseIDs := us.RegisteredCourseIDs()

	courseResults := make([]CourseResult, 0, len(registeredCourseIDs))
	myGPA := 0.0
	myCredits := 0

	for _, courseID := range registeredCourseIDs {
		course, ok := st.GetCourse(courseID)
		if !ok {
			continue
		}
		classes := course.Classes()

		// 講義は part の降順で返す
		classScores := make([]ClassScore, 0, len(classes))
		var myTotalScore int
		for i := len(classes) - 1; i >= 0; i-- {
			class := classes[i]
			submitters := class.SubmissionCount()
			sub, ok := class.GetSubmission(userID)
			if !ok || sub.Score == nil {
				classScores = append(classScores, ClassScore{
					ClassID:    class.ID,
					Part:       class.Part,
					Title:      class.Title,
					Score:      nil,
					Submitters: submitters,
				})
			} else {
				score := *sub.Score
				myTotalScore += score
				classScores = append(classScores, ClassScore{
					ClassID:    class.ID,
					Part:       class.Part,
					Title:      class.Title,
					Score:      &score,
					Submitters: submitters,
				})
			}
		}

		// この科目を履修している全学生の合計点一覧
		registeredUserIDs := course.RegisteredUserIDs()
		totals := make([]int, 0, len(registeredUserIDs))
		for _, uid := range registeredUserIDs {
			sum := 0
			for _, class := range classes {
				if sub, ok := class.GetSubmission(uid); ok && sub.Score != nil {
					sum += *sub.Score
				}
			}
			totals = append(totals, sum)
		}

		courseResults = append(courseResults, CourseResult{
			Name:             course.Name,
			Code:             course.Code,
			TotalScore:       myTotalScore,
			TotalScoreTScore: tScoreInt(myTotalScore, totals),
			TotalScoreAvg:    averageInt(totals, 0),
			TotalScoreMax:    maxInt(totals, 0),
			TotalScoreMin:    minInt(totals, 0),
			ClassScores:      classScores,
		})

		if course.Status() == StatusClosed {
			myGPA += float64(myTotalScore * int(course.Credit))
			myCredits += int(course.Credit)
		}
	}
	if myCredits > 0 {
		myGPA = myGPA / 100 / float64(myCredits)
	}

	// GPAの統計値
	// 修了済み(closed)科目を1つでも持つ学生について、closed科目だけを対象にGPAを集計する。
	// 全ユーザーを舐めるのではなく、closed科目の履修者だけを辿ることで
	// 学生総数に依存しない計算量にしている。
	type gpaAgg struct {
		score  int
		credit int
	}
	aggs := make(map[string]*gpaAgg)
	for _, course := range st.AllCourses() {
		if course.Status() != StatusClosed {
			continue
		}
		classes := course.Classes()
		for _, uid := range course.RegisteredUserIDs() {
			user := st.usersByID[uid]
			if user.Type != Student {
				continue
			}
			agg, ok := aggs[uid]
			if !ok {
				agg = &gpaAgg{}
				aggs[uid] = agg
			}
			agg.credit += int(course.Credit)
			courseTotalScore := 0
			for _, class := range classes {
				if sub, ok := class.GetSubmission(uid); ok && sub.Score != nil {
					courseTotalScore += *sub.Score
				}
			}
			agg.score += courseTotalScore * int(course.Credit)
		}
	}
	gpas := make([]float64, 0, len(aggs))
	for _, agg := range aggs {
		if agg.credit > 0 {
			gpas = append(gpas, float64(agg.score)/100/float64(agg.credit))
		}
	}

	res := GetGradeResponse{
		Summary: Summary{
			Credits:   myCredits,
			GPA:       myGPA,
			GpaTScore: tScoreFloat64(myGPA, gpas),
			GpaAvg:    averageFloat64(gpas, 0),
			GpaMax:    maxFloat64(gpas, 0),
			GpaMin:    minFloat64(gpas, 0),
		},
		CourseResults: courseResults,
	}

	return c.JSON(http.StatusOK, res)
}

// ---------- Courses API ----------

// SearchCourses GET /api/courses 科目検索
func (h *handlers) SearchCourses(c echo.Context) error {
	courseType := c.QueryParam("type")
	teacher := c.QueryParam("teacher")
	dayOfWeek := c.QueryParam("day_of_week")
	status := c.QueryParam("status")

	credit, creditOK := 0, false
	if v, err := strconv.Atoi(c.QueryParam("credit")); err == nil && v > 0 {
		credit, creditOK = v, true
	}
	period, periodOK := 0, false
	if v, err := strconv.Atoi(c.QueryParam("period")); err == nil && v > 0 {
		period, periodOK = v, true
	}

	var keywordList []string
	if keywords := c.QueryParam("keywords"); keywords != "" {
		keywordList = strings.Split(keywords, " ")
	}

	var page int
	if c.QueryParam("page") == "" {
		page = 1
	} else {
		var err error
		page, err = strconv.Atoi(c.QueryParam("page"))
		if err != nil || page <= 0 {
			return c.String(http.StatusBadRequest, "Invalid page.")
		}
	}
	limit := 20
	offset := limit * (page - 1)

	st := h.getStore()

	filtered := make([]*Course, 0)
	for _, course := range st.AllCourses() {
		if courseType != "" && string(course.Type) != courseType {
			continue
		}
		if creditOK && int(course.Credit) != credit {
			continue
		}
		if teacher != "" {
			t := st.usersByID[course.TeacherID]
			if t == nil || t.Name != teacher {
				continue
			}
		}
		if periodOK && int(course.Period) != period {
			continue
		}
		if dayOfWeek != "" && string(course.DayOfWeek) != dayOfWeek {
			continue
		}
		if len(keywordList) > 0 {
			nameMatch := true
			for _, kw := range keywordList {
				if !strings.Contains(course.Name, kw) {
					nameMatch = false
					break
				}
			}
			keywordsMatch := true
			for _, kw := range keywordList {
				if !strings.Contains(course.Keywords, kw) {
					keywordsMatch = false
					break
				}
			}
			if !nameMatch && !keywordsMatch {
				continue
			}
		}
		if status != "" && string(course.Status()) != status {
			continue
		}
		filtered = append(filtered, course)
	}

	start := offset
	if start > len(filtered) {
		start = len(filtered)
	}
	end := offset + limit + 1
	if end > len(filtered) {
		end = len(filtered)
	}
	page_ := filtered[start:end]

	res := make([]GetCourseDetailResponse, 0, len(page_))
	for _, course := range page_ {
		teacher := st.usersByID[course.TeacherID]
		res = append(res, GetCourseDetailResponse{
			ID:          course.ID,
			Code:        course.Code,
			Type:        string(course.Type),
			Name:        course.Name,
			Description: course.Description,
			Credit:      course.Credit,
			Period:      course.Period,
			DayOfWeek:   string(course.DayOfWeek),
			TeacherID:   course.TeacherID,
			Keywords:    course.Keywords,
			Status:      course.Status(),
			Teacher:     teacher.Name,
		})
	}

	var links []string
	linkURL, err := url.Parse(c.Request().URL.Path + "?" + c.Request().URL.RawQuery)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	q := linkURL.Query()
	if page > 1 {
		q.Set("page", strconv.Itoa(page-1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"prev\"", linkURL))
	}
	if len(res) > limit {
		q.Set("page", strconv.Itoa(page+1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"next\"", linkURL))
	}
	if len(links) > 0 {
		c.Response().Header().Set("Link", strings.Join(links, ","))
	}

	if len(res) == limit+1 {
		res = res[:len(res)-1]
	}

	return c.JSON(http.StatusOK, res)
}

type AddCourseRequest struct {
	Code        string     `json:"code"`
	Type        CourseType `json:"type"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Credit      int        `json:"credit"`
	Period      int        `json:"period"`
	DayOfWeek   DayOfWeek  `json:"day_of_week"`
	Keywords    string     `json:"keywords"`
}

type AddCourseResponse struct {
	ID string `json:"id"`
}

// AddCourse POST /api/courses 新規科目登録
func (h *handlers) AddCourse(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var req AddCourseRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	if req.Type != LiberalArts && req.Type != MajorSubjects {
		return c.String(http.StatusBadRequest, "Invalid course type.")
	}
	if !contains(daysOfWeek, req.DayOfWeek) {
		return c.String(http.StatusBadRequest, "Invalid day of week.")
	}

	st := h.getStore()

	newCourse := NewCourse(newULID(), req.Code, req.Type, req.Name, req.Description, uint8(req.Credit), uint8(req.Period), req.DayOfWeek, userID, req.Keywords, StatusRegistration)
	result, created := st.AddCourseIfAbsent(newCourse)
	if !created {
		if req.Type != result.Type || req.Name != result.Name || req.Description != result.Description ||
			req.Credit != int(result.Credit) || req.Period != int(result.Period) || req.DayOfWeek != result.DayOfWeek || req.Keywords != result.Keywords {
			return c.String(http.StatusConflict, "A course with the same code already exists.")
		}
	}

	return c.JSON(http.StatusCreated, AddCourseResponse{ID: result.ID})
}

type GetCourseDetailResponse struct {
	ID          string       `json:"id" db:"id"`
	Code        string       `json:"code" db:"code"`
	Type        string       `json:"type" db:"type"`
	Name        string       `json:"name" db:"name"`
	Description string       `json:"description" db:"description"`
	Credit      uint8        `json:"credit" db:"credit"`
	Period      uint8        `json:"period" db:"period"`
	DayOfWeek   string       `json:"day_of_week" db:"day_of_week"`
	TeacherID   string       `json:"-" db:"teacher_id"`
	Keywords    string       `json:"keywords" db:"keywords"`
	Status      CourseStatus `json:"status" db:"status"`
	Teacher     string       `json:"teacher" db:"teacher"`
}

// GetCourseDetail GET /api/courses/:courseID 科目詳細の取得
func (h *handlers) GetCourseDetail(c echo.Context) error {
	courseID := c.Param("courseID")

	st := h.getStore()
	course, ok := st.GetCourse(courseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}
	teacher := st.usersByID[course.TeacherID]

	res := GetCourseDetailResponse{
		ID:          course.ID,
		Code:        course.Code,
		Type:        string(course.Type),
		Name:        course.Name,
		Description: course.Description,
		Credit:      course.Credit,
		Period:      course.Period,
		DayOfWeek:   string(course.DayOfWeek),
		TeacherID:   course.TeacherID,
		Keywords:    course.Keywords,
		Status:      course.Status(),
		Teacher:     teacher.Name,
	}

	return c.JSON(http.StatusOK, res)
}

type SetCourseStatusRequest struct {
	Status CourseStatus `json:"status"`
}

// SetCourseStatus PUT /api/courses/:courseID/status 科目のステータスを変更
func (h *handlers) SetCourseStatus(c echo.Context) error {
	courseID := c.Param("courseID")

	var req SetCourseStatusRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	st := h.getStore()
	course, ok := st.GetCourse(courseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}

	course.SetStatus(req.Status)

	return c.NoContent(http.StatusOK)
}

type GetClassResponse struct {
	ID               string `json:"id"`
	Part             uint8  `json:"part"`
	Title            string `json:"title"`
	Description      string `json:"description"`
	SubmissionClosed bool   `json:"submission_closed"`
	Submitted        bool   `json:"submitted"`
}

// GetClasses GET /api/courses/:courseID/classes 科目に紐づく講義一覧の取得
func (h *handlers) GetClasses(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")

	st := h.getStore()
	course, ok := st.GetCourse(courseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}

	classes := course.Classes()
	res := make([]GetClassResponse, 0, len(classes))
	for _, class := range classes {
		_, submitted := class.GetSubmission(userID)
		res = append(res, GetClassResponse{
			ID:               class.ID,
			Part:             class.Part,
			Title:            class.Title,
			Description:      class.Description,
			SubmissionClosed: class.Closed(),
			Submitted:        submitted,
		})
	}

	return c.JSON(http.StatusOK, res)
}

type AddClassRequest struct {
	Part        uint8  `json:"part"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type AddClassResponse struct {
	ClassID string `json:"class_id"`
}

// AddClass POST /api/courses/:courseID/classes 新規講義(&課題)追加
func (h *handlers) AddClass(c echo.Context) error {
	courseID := c.Param("courseID")

	var req AddClassRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	st := h.getStore()
	course, ok := st.GetCourse(courseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}
	if course.Status() != StatusInProgress {
		return c.String(http.StatusBadRequest, "This course is not in-progress.")
	}

	classID := newULID()
	class, created := course.AddClassIfAbsent(classID, req.Part, req.Title, req.Description)
	if !created {
		if req.Title != class.Title || req.Description != class.Description {
			return c.String(http.StatusConflict, "A class with the same part already exists.")
		}
		return c.JSON(http.StatusCreated, AddClassResponse{ClassID: class.ID})
	}
	st.registerClassStructure(class)

	return c.JSON(http.StatusCreated, AddClassResponse{ClassID: class.ID})
}

// SubmitAssignment POST /api/courses/:courseID/classes/:classID/assignments 課題の提出
func (h *handlers) SubmitAssignment(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")
	classID := c.Param("classID")

	st := h.getStore()

	course, ok := st.GetCourse(courseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}
	if course.Status() != StatusInProgress {
		return c.String(http.StatusBadRequest, "This course is not in progress.")
	}

	us := st.GetUserState(userID)
	if !us.IsRegistered(courseID) {
		return c.String(http.StatusBadRequest, "You have not taken this course.")
	}

	class, ok := st.GetClass(classID)
	if !ok || class.CourseID != courseID {
		return c.String(http.StatusNotFound, "No such class.")
	}
	if class.Closed() {
		return c.String(http.StatusBadRequest, "Submission has been closed for this class.")
	}

	file, header, err := c.Request().FormFile("file")
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid file.")
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	class.UpsertSubmission(userID, header.Filename, data)

	return c.NoContent(http.StatusNoContent)
}

type Score struct {
	UserCode string `json:"user_code"`
	Score    int    `json:"score"`
}

// RegisterScores PUT /api/courses/:courseID/classes/:classID/assignments/scores 採点結果登録
func (h *handlers) RegisterScores(c echo.Context) error {
	classID := c.Param("classID")

	st := h.getStore()
	class, ok := st.GetClass(classID)
	if !ok {
		return c.String(http.StatusNotFound, "No such class.")
	}
	if !class.Closed() {
		return c.String(http.StatusBadRequest, "This assignment is not closed yet.")
	}

	var req []Score
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	for _, score := range req {
		user, ok := st.usersByCode[score.UserCode]
		if !ok {
			continue
		}
		class.SetScore(user.ID, score.Score)
	}

	return c.NoContent(http.StatusNoContent)
}

// DownloadSubmittedAssignments GET /api/courses/:courseID/classes/:classID/assignments/export 提出済みの課題ファイルをzip形式で一括ダウンロード
func (h *handlers) DownloadSubmittedAssignments(c echo.Context) error {
	classID := c.Param("classID")

	st := h.getStore()
	class, ok := st.GetClass(classID)
	if !ok {
		return c.String(http.StatusNotFound, "No such class.")
	}

	entries := class.AllSubmissions()
	class.Close()

	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	for _, e := range entries {
		user := st.usersByID[e.UserID]
		w, err := zw.Create(user.Code + "-" + e.FileName)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		if _, err := w.Write(e.Data); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}
	if err := zw.Close(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Blob(http.StatusOK, "application/zip", buf.Bytes())
}

// ---------- Announcement API ----------

type AnnouncementWithoutDetail struct {
	ID         string `json:"id" db:"id"`
	CourseID   string `json:"course_id" db:"course_id"`
	CourseName string `json:"course_name" db:"course_name"`
	Title      string `json:"title" db:"title"`
	Unread     bool   `json:"unread" db:"unread"`
}

type GetAnnouncementsResponse struct {
	UnreadCount   int                         `json:"unread_count"`
	Announcements []AnnouncementWithoutDetail `json:"announcements"`
}

// GetAnnouncementList GET /api/announcements お知らせ一覧取得
func (h *handlers) GetAnnouncementList(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseFilter := c.QueryParam("course_id")

	var page int
	if c.QueryParam("page") == "" {
		page = 1
	} else {
		page, err = strconv.Atoi(c.QueryParam("page"))
		if err != nil || page <= 0 {
			return c.String(http.StatusBadRequest, "Invalid page.")
		}
	}
	limit := 20
	offset := limit * (page - 1)

	st := h.getStore()
	us := st.GetUserState(userID)

	filtered := us.AnnouncementsDesc(st, courseFilter)

	start := offset
	if start > len(filtered) {
		start = len(filtered)
	}
	end := offset + limit + 1
	if end > len(filtered) {
		end = len(filtered)
	}
	page_ := filtered[start:end]

	announcements := make([]AnnouncementWithoutDetail, 0, len(page_))
	for _, ua := range page_ {
		ann, ok := st.GetAnnouncement(ua.AnnouncementID)
		if !ok {
			continue
		}
		announcements = append(announcements, AnnouncementWithoutDetail{
			ID:         ann.ID,
			CourseID:   ann.CourseID,
			CourseName: ann.CourseName,
			Title:      ann.Title,
			Unread:     ua.Unread(),
		})
	}

	unreadCount := us.UnreadCount()

	var links []string
	linkURL, err := url.Parse(c.Request().URL.Path + "?" + c.Request().URL.RawQuery)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	q := linkURL.Query()
	if page > 1 {
		q.Set("page", strconv.Itoa(page-1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"prev\"", linkURL))
	}
	if len(announcements) > limit {
		q.Set("page", strconv.Itoa(page+1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"next\"", linkURL))
	}
	if len(links) > 0 {
		c.Response().Header().Set("Link", strings.Join(links, ","))
	}

	if len(announcements) == limit+1 {
		announcements = announcements[:len(announcements)-1]
	}

	return c.JSON(http.StatusOK, GetAnnouncementsResponse{
		UnreadCount:   unreadCount,
		Announcements: announcements,
	})
}

type AddAnnouncementRequest struct {
	ID       string `json:"id"`
	CourseID string `json:"course_id"`
	Title    string `json:"title"`
	Message  string `json:"message"`
}

// AddAnnouncement POST /api/announcements 新規お知らせ追加
func (h *handlers) AddAnnouncement(c echo.Context) error {
	var req AddAnnouncementRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	st := h.getStore()
	course, ok := st.GetCourse(req.CourseID)
	if !ok {
		return c.String(http.StatusNotFound, "No such course.")
	}

	newAnn := &Announcement{
		ID:         req.ID,
		CourseID:   req.CourseID,
		CourseName: course.Name,
		Title:      req.Title,
		Message:    req.Message,
	}
	result, created := st.AddAnnouncementIfAbsent(newAnn)
	if !created {
		if result.CourseID != req.CourseID || result.Title != req.Title || result.Message != req.Message {
			return c.String(http.StatusConflict, "An announcement with the same id already exists.")
		}
		return c.NoContent(http.StatusCreated)
	}

	for _, uid := range course.RegisteredUserIDs() {
		if us := st.GetUserState(uid); us != nil {
			us.AddAnnouncement(req.ID, true)
		}
	}

	return c.NoContent(http.StatusCreated)
}

type AnnouncementDetail struct {
	ID         string `json:"id" db:"id"`
	CourseID   string `json:"course_id" db:"course_id"`
	CourseName string `json:"course_name" db:"course_name"`
	Title      string `json:"title" db:"title"`
	Message    string `json:"message" db:"message"`
	Unread     bool   `json:"unread" db:"unread"`
}

// GetAnnouncementDetail GET /api/announcements/:announcementID お知らせ詳細取得
func (h *handlers) GetAnnouncementDetail(c echo.Context) error {
	userID, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	announcementID := c.Param("announcementID")

	st := h.getStore()
	us := st.GetUserState(userID)

	wasUnread, ok := us.MarkAnnouncementRead(announcementID)
	if !ok {
		return c.String(http.StatusNotFound, "No such announcement.")
	}
	ann, ok := st.GetAnnouncement(announcementID)
	if !ok {
		return c.String(http.StatusNotFound, "No such announcement.")
	}

	res := AnnouncementDetail{
		ID:         ann.ID,
		CourseID:   ann.CourseID,
		CourseName: ann.CourseName,
		Title:      ann.Title,
		Message:    ann.Message,
		Unread:     wasUnread,
	}

	return c.JSON(http.StatusOK, res)
}
