package domain

import "context"

const (
	// Unknown la trang thai chua xac dinh danh tinh may bay.
	Unknown int = 0
	// Friendly la may bay phe ta/duoc nhan dien than thien.
	Friendly int = 1
	// Hostile la may bay doi phuong/duoc nhan dien thu dich.
	Hostile int = 2
)

// AircraftIdentity dinh danh duy nhat mot track may bay.
// Callsign co the xuat hien lai o nhieu phien, nen DetectionTime duoc dung
// kem theo de phan biet cac lan phat hien khac nhau cua cung mot callsign.
type AircraftIdentity struct {
	Callsign string
	// DetectionTime la thoi diem track duoc phat hien lan dau.
	DetectionTime int64
}

// Aircraft la domain model chinh cho may bay trong he thong.
// Struct nay vua dung cho trang thai hien tai, vua dung de ghi tung diem vi tri
// vao history/cold storage.
type Aircraft struct {
	Callsign string
	// DetectionTime ket hop voi Callsign de tao khoa cua mot track.
	DetectionTime int64
	// Category la nhom nhan dien: Unknown, Friendly hoac Hostile.
	Category int
	// Mode3A la ma transponder Mode 3/A cua may bay.
	Mode3A string
	// Classification mo ta loai/nhom muc tieu theo nguon du lieu dau vao.
	Classification string
	// LastLat, LastLng, LastAlt la toa do moi nhat cua track.
	LastLat float64
	LastLng float64
	LastAlt float64
	// Speed va Heading la van toc va huong bay tai thoi diem LastTimestamp.
	Speed   float64
	Heading float64
	// LastTimestamp la thoi diem cua ban tin vi tri moi nhat.
	LastTimestamp int64
	// IsPermanent danh dau diem archived_position da duoc pin, khong bi cleanup.
	IsPermanent bool
}

// Position la diem vi tri toi gian dung cho playback/trajectory.
type Position struct {
	Lat     float64
	Lng     float64
	Alt     float64
	Speed   float64
	Heading float64
	// Timestamp la thoi diem ghi nhan diem vi tri.
	Timestamp int64
	// IsPermanent cho biet diem playback nay co duoc pin trong cold data khong.
	IsPermanent bool
}

// FlightPlayback gom toan bo cac diem can phat lai cho mot track may bay.
type FlightPlayback struct {
	Callsign string
	// DetectionTime giup phan biet track khi cung Callsign xuat hien nhieu lan.
	DetectionTime int64
	// Category la nhom nhan dien cua track: Unknown, Friendly hoac Hostile.
	Category int
	// Classification mo ta loai/nhom muc tieu theo nguon du lieu dau vao.
	Classification string
	Positions      []Position
}

// AircraftRepository la hop dong truy cap du lieu cho aircraft.
// Tang implementation hien tai ghi hot data vao bang aircraft/history_position
// va cold data vao bang archived_flight_summary/archived_position.
type AircraftRepository interface {
	// SaveAircraftColdData luu du lieu archived de playback va truy van lich su dai han.
	SaveAircraftColdData(ctx context.Context, aircrafts []Aircraft) error
	// SaveAircraftHotData cap nhat trang thai hien tai va history position gan nhat.
	SaveAircraftHotData(ctx context.Context, aircrafts []Aircraft) error
	// MarkPositionsAsPermanent pin cac diem archived_position trong khoang thoi gian.
	MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error
	// GetAircraft lay trang thai moi nhat cua mot track.
	GetAircraft(ctx context.Context, callsign string, detectionTime int64) (Aircraft, error)
	// DeleteAircrafts xoa cac aircraft theo callsign.
	DeleteAircrafts(ctx context.Context, callsign []string) error
	// GetHistoryPositions lay cac diem history_position cua mot track.
	GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]Aircraft, error)
	// CleanupExpiredPositions xoa archived_position qua han va chua duoc pin.
	CleanupExpiredPositions(ctx context.Context, cutoffTimestamp int64) error
	// GetAllArchivedPositionsByTimeWindow lay tat ca diem archived trong khoang thoi gian.
	GetAllArchivedPositionsByTimeWindow(ctx context.Context, fromTs int64, toTs int64) ([]Aircraft, error)
	// GetAircraftsByTimeWindow lay danh sach track co du lieu trong khoang thoi gian.
	GetAircraftsByTimeWindow(ctx context.Context, fromTs, toTs int64) ([]AircraftIdentity, error)
	// GetPlaybackDataByTimeWindow lay du lieu playback day du cho cac track da chon.
	GetPlaybackDataByTimeWindow(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs int64) ([]FlightPlayback, error)
	// GetPlaybackDataBySession lay playback theo session, co the lay mau theo sampleIntervalMs.
	GetPlaybackDataBySession(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs, sampleIntervalMs int64) ([]FlightPlayback, error)
}

// AircraftUsecase la hop dong nghiep vu cho luong aircraft.
// Tang delivery goi interface nay de xu ly update live, truy van history va playback.
type AircraftUsecase interface {
	// ProcessAircraftFrame(ctx context.Context, aircrafts []Aircraft) error
	// SaveAircraftColdData cho phep ghi cold data truc tiep khi can.
	SaveAircraftColdData(ctx context.Context, aircrafts []Aircraft) error
	// ProcessAircraftUpdate xu ly mot update live tu NATS/protobuf.
	ProcessAircraftUpdate(ctx context.Context, aircraft Aircraft)
	// GetAircraft lay trang thai moi nhat cua mot track.
	GetAircraft(ctx context.Context, callsign string, detectionTime int64) (Aircraft, error)
	// DeleteAircrafts xoa cac aircraft theo callsign.
	DeleteAircrafts(ctx context.Context, callsign []string) error
	// GetHistoryPositions lay lich su diem bay cua mot track.
	GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]Aircraft, error)
	// MarkPositionsAsPermanent pin cac diem archived de giu lai khi cleanup.
	MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error
	// GetGlobalPlaybackData lay playback cho toan bo track trong khoang thoi gian.
	GetGlobalPlaybackData(ctx context.Context, fromTs int64, toTs int64) ([]FlightPlayback, error)
	// GetAircraftsByTimeWindow lay cac track co diem archived trong khoang thoi gian.
	GetAircraftsByTimeWindow(ctx context.Context, fromTs, toTs int64) ([]AircraftIdentity, error)
	// GetPlaybackDataByTimeWindow lay playback day du cho cac track duoc chon.
	GetPlaybackDataByTimeWindow(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs int64) ([]FlightPlayback, error)
	// GetPlaybackDataBySession lay playback trong session, co the giam mat do mau.
	GetPlaybackDataBySession(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs, sampleIntervalMs int64) ([]FlightPlayback, error)
}

// NatsPublisher dinh nghia cong publish frame live ra NATS.
type NatsPublisher interface {
	// PublishLiveFrame gui payload da marshal len subject live frame.
	PublishLiveFrame(data []byte) error
}
