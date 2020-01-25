package scalers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	mySQLMetricName      = "queryValue"
	defaultMySQLPassword = ""
)

type mySQLScaler struct {
	metadata   *mySQLMetadata
	connection *sql.DB
}

type mySQLMetadata struct {
	dsnConnStr string // Database connection string
	username   string
	password   string
	host       string
	port       string
	query      string
	dbname     string
}

var mySQLLog = logf.Log.WithName("mysql_scaler")

// NewMySQLScaler creates a new MySQL scaler
func NewMySQLScaler(resolvedEnv, metadata, authParams map[string]string) (Scaler, error) {
	meta, err := parseMySQLMetadata(resolvedEnv, metadata, authParams)
	if err != nil {
		return nil, fmt.Errorf("error parsing MySQL metadata: %s", err)
	}

	conn, err := newMySQLConnection(meta)
	if err != nil {
		return nil, fmt.Errorf("error establishing MySQL dsnConnStr: %s", err)
	}
	return &mySQLScaler{
		metadata:   meta,
		connection: conn,
	}, nil
}

func parseMySQLMetadata(resolvedEnv, metadata, authParams map[string]string) (*mySQLMetadata, error) {
	meta := mySQLMetadata{}

	if val, ok := metadata["query"]; ok {
		meta.query = val
	} else {
		return nil, fmt.Errorf("no query given")
	}

	if val, ok := authParams["dsnConnStr"]; ok {
		meta.dsnConnStr = val
	} else if val, ok := metadata["dsnConnStr"]; ok {
		hostSetting := val

		if val, ok := resolvedEnv[hostSetting]; ok {
			meta.dsnConnStr = val
		}
	} else {
		meta.dsnConnStr = ""
		if val, ok := metadata["host"]; ok {
			meta.host = val
		} else {
			return nil, fmt.Errorf("no host given")
		}
		if val, ok := metadata["port"]; ok {
			meta.port = val
		} else {
			return nil, fmt.Errorf("no port given")
		}

		if val, ok := metadata["username"]; ok {
			meta.username = val
		} else {
			return nil, fmt.Errorf("no username given")
		}
		if val, ok := metadata["dbname"]; ok {
			meta.dbname = val
		} else {
			return nil, fmt.Errorf("no dbname given")
		}
		meta.password = defaultMySQLPassword
		if val, ok := authParams["password"]; ok {
			meta.password = val
		} else if val, ok := metadata["password"]; ok && val != "" {
			if pass, ok := resolvedEnv[val]; ok {
				meta.password = pass
			}
		}
	}

	return &meta, nil
}

// newMySQLConnection creates MySQL db connection
func newMySQLConnection(meta *mySQLMetadata) (*sql.DB, error) {
	var connStr string

	if meta.dsnConnStr != "" {
		connStr = meta.dsnConnStr
	} else {
		// Build connection str
		config := mysql.NewConfig()
		config.Addr = fmt.Sprintf("%s:%s", meta.host, meta.port)
		config.DBName = meta.dbname
		config.Passwd = meta.password
		config.User = meta.username
		config.Net = "tcp"
		connStr = config.FormatDSN()
	}
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		mySQLLog.Error(err, fmt.Sprintf("Found error when opening connection: %s", err))
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		mySQLLog.Error(err, fmt.Sprintf("Found error when pinging databse: %s", err))
		return nil, err
	}
	return db, nil
}

// Close disposes of MySQL connections
func (s *mySQLScaler) Close() error {
	err := s.connection.Close()
	if err != nil {
		mySQLLog.Error(err, "Error closing MySQL connection")
		return err
	}
	return nil
}

// IsActive returns true if there are pending messages to be processed
func (s *mySQLScaler) IsActive(ctx context.Context) (bool, error) {
	messages, err := s.getQueryResult()
	if err != nil {
		mySQLLog.Error(err, fmt.Sprintf("Error inspecting MySQL: %s", err))
		return false, err
	}
	return messages > 0, nil
}

// getQueryResult returns result of the scaler query
func (s *mySQLScaler) getQueryResult() (int, error) {
	var value int
	err := s.connection.QueryRow(s.metadata.query).Scan(&value)
	if err != nil {
		mySQLLog.Error(err, fmt.Sprintf("Could not query MySQL database: %s", err))
		return 0, err
	}
	return value, nil
}

// GetMetricSpecForScaling returns the MetricSpec for the Horizontal Pod Autoscaler
func (s *mySQLScaler) GetMetricSpecForScaling() []v2beta1.MetricSpec {
	targetListLengthQty := resource.NewQuantity(1, resource.DecimalSI)
	externalMetric := &v2beta1.ExternalMetricSource{
		MetricName:         mySQLMetricName,
		TargetAverageValue: targetListLengthQty,
	}
	metricSpec := v2beta1.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2beta1.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *mySQLScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	num, err := s.getQueryResult()
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, fmt.Errorf("error inspecting MySQL: %s", err)
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: mySQLMetricName,
		Value:      *resource.NewQuantity(int64(num), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}
