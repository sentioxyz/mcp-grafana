package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/gtime"
	mcpgrafana "github.com/grafana/mcp-grafana"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	// DefaultClickHouseLimit is the default number of rows to return if not specified
	DefaultClickHouseLimit = 1000

	// MaxClickHouseLimit is the maximum number of rows that can be requested
	MaxClickHouseLimit = 10000
)

// ClickHouseClient wraps HTTP client for ClickHouse datasource queries via Grafana /api/ds/query
type ClickHouseClient struct {
	httpClient     *http.Client
	baseURL        string
	datasourceUID  string
	datasourceType string
}

// newClickHouseClient creates a new ClickHouse client for the given datasource UID
func newClickHouseClient(ctx context.Context, uid string) (*ClickHouseClient, error) {
	// 1. Check datasource exists using getDatasourceByUID
	ds, err := getDatasourceByUID(ctx, GetDatasourceByUIDParams{UID: uid})
	if err != nil {
		return nil, err
	}

	// 2. Get Grafana config from context
	cfg := mcpgrafana.GrafanaConfigFromContext(ctx)

	// 3. Build query URL: /api/ds/query
	queryURL := fmt.Sprintf("%s/api/ds/query", strings.TrimRight(cfg.URL, "/"))

	// 4. Setup HTTP client with TLS config if needed
	var transport = http.DefaultTransport
	if tlsConfig := cfg.TLSConfig; tlsConfig != nil {
		var err error
		transport, err = tlsConfig.HTTPTransport(transport.(*http.Transport))
		if err != nil {
			return nil, fmt.Errorf("failed to create custom transport: %w", err)
		}
	}

	// 5. Add auth support (access token, API key, or basic auth)
	transport = NewAuthRoundTripper(transport, cfg.AccessToken, cfg.IDToken, cfg.APIKey, cfg.BasicAuth)

	// 6. Wrap with org ID support
	transport = mcpgrafana.NewOrgIDRoundTripper(transport, cfg.OrgID)

	client := &http.Client{
		Transport: mcpgrafana.NewUserAgentTransport(transport),
		Timeout:   30 * time.Second, // Prevent hanging on slow queries
	}

	return &ClickHouseClient{
		httpClient:     client,
		baseURL:        queryURL,
		datasourceUID:  uid,
		datasourceType: ds.Type,
	}, nil
}

// ClickHouseQueryRequest represents a Grafana query API request for ClickHouse
type ClickHouseQueryRequest struct {
	Queries []ClickHouseQuery `json:"queries"`
	From    string            `json:"from"`
	To      string            `json:"to"`
}

// ClickHouseQuery represents a single query in the request
type ClickHouseQuery struct {
	Datasource struct {
		Type string `json:"type"`
		UID  string `json:"uid"`
	} `json:"datasource"`
	EditorType   string                 `json:"editorType"`
	Format       int                    `json:"format"`
	Meta         map[string]interface{} `json:"meta"`
	QueryType    string                 `json:"queryType"`
	RawSQL       string                 `json:"rawSql"`
	RefID        string                 `json:"refId"`
	IntervalMs   int                    `json:"intervalMs,omitempty"`
	MaxDataPoints int                   `json:"maxDataPoints,omitempty"`
}

// GrafanaQueryResponse represents the response from /api/ds/query
type GrafanaQueryResponse struct {
	Results map[string]QueryResult `json:"results"`
}

// QueryResult represents a single query result
type QueryResult struct {
	Frames []DataFrame `json:"frames"`
	Error  string      `json:"error,omitempty"`
}

// DataFrame represents a data frame in the response
type DataFrame struct {
	Schema struct {
		Fields []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"fields"`
	} `json:"schema"`
	Data struct {
		Values [][]interface{} `json:"values"`
	} `json:"data"`
}

// executeQuery executes a SQL query against ClickHouse via Grafana /api/ds/query
func (c *ClickHouseClient) executeQuery(ctx context.Context, rawSQL string, from, to time.Time, limit int) ([]map[string]interface{}, error) {
	// Build query request
	queryReq := ClickHouseQueryRequest{
		Queries: []ClickHouseQuery{
			{
				EditorType: "sql",
				Format:     2,
				Meta: map[string]interface{}{
					"builderOptions": map[string]interface{}{
						"columns":   []string{},
						"database":  "",
						"limit":     limit,
						"mode":      "list",
						"queryType": "table",
						"table":     "",
					},
				},
				QueryType: "logs",
				RawSQL:    rawSQL,
				RefID:     "A",
			},
		},
		From: fmt.Sprintf("%d", from.UnixMilli()),
		To:   fmt.Sprintf("%d", to.UnixMilli()),
	}

	// Set datasource info
	queryReq.Queries[0].Datasource.Type = c.datasourceType
	queryReq.Queries[0].Datasource.UID = c.datasourceUID

	// Marshal request to JSON
	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return nil, fmt.Errorf("marshalling request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Check for non-200 status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ClickHouse API returned status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	body := io.LimitReader(resp.Body, 1024*1024*48) // 48MB limit
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	// Parse JSON response
	var grafanaResp GrafanaQueryResponse
	if err := json.Unmarshal(bodyBytes, &grafanaResp); err != nil {
		return nil, fmt.Errorf("unmarshalling response (content: %s): %w", string(bodyBytes), err)
	}

	// Extract data from response
	result, ok := grafanaResp.Results["A"]
	if !ok {
		return nil, fmt.Errorf("no result found for query A")
	}

	if result.Error != "" {
		return nil, fmt.Errorf("query error: %s", result.Error)
	}

	if len(result.Frames) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Convert data frames to rows
	frame := result.Frames[0]
	rows := make([]map[string]interface{}, 0)

	if len(frame.Data.Values) == 0 {
		return rows, nil
	}

	// Get number of rows
	numRows := len(frame.Data.Values[0])

	// Convert columnar data to rows
	for i := 0; i < numRows; i++ {
		row := make(map[string]interface{})
		for j, field := range frame.Schema.Fields {
			if j < len(frame.Data.Values) && i < len(frame.Data.Values[j]) {
				row[field.Name] = frame.Data.Values[j][i]
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// parseClickHouseTime parses a time string using Grafana's gtime library
// Supports RFC3339, Unix timestamps, and Grafana-style relative times ("now-1h")
func parseClickHouseTime(timeStr string) (time.Time, error) {
	tr := gtime.TimeRange{
		From: timeStr,
		Now:  time.Now(),
	}
	return tr.ParseFrom()
}

// enforceRowLimit ensures a row limit value is within acceptable bounds
func enforceRowLimit(requestedLimit int) int {
	if requestedLimit <= 0 {
		return DefaultClickHouseLimit
	}
	if requestedLimit > MaxClickHouseLimit {
		return MaxClickHouseLimit
	}
	return requestedLimit
}

// QueryClickHouseLogsParams defines the parameters for querying ClickHouse logs
type QueryClickHouseLogsParams struct {
	DatasourceUID string `json:"datasourceUid" jsonschema:"required,description=The UID of the ClickHouse datasource"`
	Query         string `json:"query" jsonschema:"required,description=SQL SELECT statement to execute. Use $__timeFilter macro for time filtering (e.g.\\, 'WHERE $__timeFilter(timestamp_column)')"`
	From          string `json:"from,omitempty" jsonschema:"description=Start time (e.g.\\, 'now-1h'\\, RFC3339\\, Unix timestamp). Defaults to 1 hour ago"`
	To            string `json:"to,omitempty" jsonschema:"description=End time (e.g.\\, 'now'\\, RFC3339\\, Unix timestamp). Defaults to now"`
	Limit         int    `json:"limit,omitempty" jsonschema:"default=1000,description=Maximum rows to return (max: 10000)"`
}

// QueryClickHouseLogsResponse defines the response structure
type QueryClickHouseLogsResponse struct {
	Rows          []map[string]interface{} `json:"rows"`
	RowCount      int                      `json:"rowCount"`
	Limited       bool                     `json:"limited"`
	ExecutionTime string                   `json:"executionTime"`
}

// queryClickHouseLogs executes a SQL query against a ClickHouse datasource for log data
func queryClickHouseLogs(ctx context.Context, args QueryClickHouseLogsParams) (*QueryClickHouseLogsResponse, error) {
	client, err := newClickHouseClient(ctx, args.DatasourceUID)
	if err != nil {
		return nil, fmt.Errorf("creating ClickHouse client: %w", err)
	}

	// Enforce row limit
	limit := enforceRowLimit(args.Limit)

	// Parse from/to times
	var fromTime, toTime time.Time

	if args.From != "" {
		fromTime, err = parseClickHouseTime(args.From)
		if err != nil {
			return nil, fmt.Errorf("parsing from time: %w", err)
		}
	} else {
		// Default to 1 hour ago
		fromTime = time.Now().Add(-1 * time.Hour)
	}

	if args.To != "" {
		toTime, err = parseClickHouseTime(args.To)
		if err != nil {
			return nil, fmt.Errorf("parsing to time: %w", err)
		}
	} else {
		// Default to now
		toTime = time.Now()
	}

	// Validate time range
	if !fromTime.Before(toTime) {
		return nil, fmt.Errorf("invalid time range: from (%s) must be before to (%s)", fromTime, toTime)
	}

	// Execute query via Grafana query API
	startTime := time.Now()
	rows, err := client.executeQuery(ctx, args.Query, fromTime, toTime, limit)
	if err != nil {
		return nil, fmt.Errorf("querying ClickHouse datasource %s: %w", args.DatasourceUID, err)
	}
	executionTime := time.Since(startTime)

	// Check if results were limited
	limited := len(rows) >= limit

	return &QueryClickHouseLogsResponse{
		Rows:          rows,
		RowCount:      len(rows),
		Limited:       limited,
		ExecutionTime: fmt.Sprintf("%.0fms", executionTime.Seconds()*1000),
	}, nil
}

// QueryClickHouseLogs is a tool for querying logs from ClickHouse
var QueryClickHouseLogs = mcpgrafana.MustTool(
	"query_clickhouse_logs",
	"Execute SQL queries against ClickHouse datasources for log data via Grafana's query API. Supports Grafana-style time expressions ('now-1h', 'now-24h') passed as from/to parameters. For time filtering in your SQL, use the $__timeFilter macro (e.g., 'WHERE $__timeFilter(timestamp_column)'). Enforces row limits to prevent context overflow. Returns log entries as JSON objects with execution metadata.",
	queryClickHouseLogs,
	mcp.WithTitleAnnotation("Query ClickHouse logs"),
	mcp.WithIdempotentHintAnnotation(true),
	mcp.WithReadOnlyHintAnnotation(true),
)

// ListClickHouseTablesParams defines the parameters for listing ClickHouse tables
type ListClickHouseTablesParams struct {
	DatasourceUID string `json:"datasourceUid" jsonschema:"required,description=The UID of the ClickHouse datasource"`
	Database      string `json:"database,omitempty" jsonschema:"description=Filter to specific database"`
}

// ListClickHouseTablesResponse defines the response structure
type ListClickHouseTablesResponse struct {
	Databases []string `json:"databases,omitempty"`
	Tables    []struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Engine   string `json:"engine"`
	} `json:"tables,omitempty"`
}

// listClickHouseTables lists available databases and tables in a ClickHouse datasource
func listClickHouseTables(ctx context.Context, args ListClickHouseTablesParams) (*ListClickHouseTablesResponse, error) {
	client, err := newClickHouseClient(ctx, args.DatasourceUID)
	if err != nil {
		return nil, fmt.Errorf("creating ClickHouse client: %w", err)
	}

	// Use default time range for metadata queries
	now := time.Now()
	from := now.Add(-1 * time.Hour)

	if args.Database == "" {
		// List all databases
		rows, err := client.executeQuery(ctx, "SHOW DATABASES", from, now, 1000)
		if err != nil {
			return nil, fmt.Errorf("listing databases: %w", err)
		}

		databases := make([]string, 0, len(rows))
		for _, row := range rows {
			if name, ok := row["name"].(string); ok {
				databases = append(databases, name)
			}
		}

		return &ListClickHouseTablesResponse{
			Databases: databases,
		}, nil
	}

	// List tables in specific database
	query := fmt.Sprintf("SELECT database, name, engine FROM system.tables WHERE database = '%s'", args.Database)
	rows, err := client.executeQuery(ctx, query, from, now, 1000)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}

	tables := make([]struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Engine   string `json:"engine"`
	}, 0, len(rows))

	for _, row := range rows {
		table := struct {
			Database string `json:"database"`
			Name     string `json:"name"`
			Engine   string `json:"engine"`
		}{}

		if database, ok := row["database"].(string); ok {
			table.Database = database
		}
		if name, ok := row["name"].(string); ok {
			table.Name = name
		}
		if engine, ok := row["engine"].(string); ok {
			table.Engine = engine
		}

		tables = append(tables, table)
	}

	return &ListClickHouseTablesResponse{
		Tables: tables,
	}, nil
}

// ListClickHouseTables is a tool for listing ClickHouse databases and tables
var ListClickHouseTables = mcpgrafana.MustTool(
	"list_clickhouse_tables",
	"List available databases and tables in a ClickHouse datasource. If no database is specified, returns all databases. If a database is specified, returns tables in that database with their engine types.",
	listClickHouseTables,
	mcp.WithTitleAnnotation("List ClickHouse tables"),
	mcp.WithIdempotentHintAnnotation(true),
	mcp.WithReadOnlyHintAnnotation(true),
)

// DescribeClickHouseTableParams defines the parameters for describing a ClickHouse table
type DescribeClickHouseTableParams struct {
	DatasourceUID string `json:"datasourceUid" jsonschema:"required,description=The UID of the ClickHouse datasource"`
	Database      string `json:"database" jsonschema:"required,description=Database name"`
	Table         string `json:"table" jsonschema:"required,description=Table name"`
}

// DescribeClickHouseTableResponse defines the response structure
type DescribeClickHouseTableResponse struct {
	Columns []struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Default string `json:"default"`
		Comment string `json:"comment"`
	} `json:"columns"`
}

// describeClickHouseTable gets schema information for a ClickHouse table
func describeClickHouseTable(ctx context.Context, args DescribeClickHouseTableParams) (*DescribeClickHouseTableResponse, error) {
	client, err := newClickHouseClient(ctx, args.DatasourceUID)
	if err != nil {
		return nil, fmt.Errorf("creating ClickHouse client: %w", err)
	}

	// Use default time range for metadata queries
	now := time.Now()
	from := now.Add(-1 * time.Hour)

	// Query system.columns for table schema
	query := fmt.Sprintf(
		"SELECT name, type, default_expression, comment FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position",
		args.Database, args.Table,
	)

	rows, err := client.executeQuery(ctx, query, from, now, 1000)
	if err != nil {
		return nil, fmt.Errorf("describing table: %w", err)
	}

	columns := make([]struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Default string `json:"default"`
		Comment string `json:"comment"`
	}, 0, len(rows))

	for _, row := range rows {
		column := struct {
			Name    string `json:"name"`
			Type    string `json:"type"`
			Default string `json:"default"`
			Comment string `json:"comment"`
		}{}

		if name, ok := row["name"].(string); ok {
			column.Name = name
		}
		if typ, ok := row["type"].(string); ok {
			column.Type = typ
		}
		if def, ok := row["default_expression"].(string); ok {
			column.Default = def
		}
		if comment, ok := row["comment"].(string); ok {
			column.Comment = comment
		}

		columns = append(columns, column)
	}

	return &DescribeClickHouseTableResponse{
		Columns: columns,
	}, nil
}

// DescribeClickHouseTable is a tool for getting ClickHouse table schema
var DescribeClickHouseTable = mcpgrafana.MustTool(
	"describe_clickhouse_table",
	"Get schema information for a ClickHouse table, including column names, types, default values, and comments. Useful for understanding table structure before writing queries.",
	describeClickHouseTable,
	mcp.WithTitleAnnotation("Describe ClickHouse table"),
	mcp.WithIdempotentHintAnnotation(true),
	mcp.WithReadOnlyHintAnnotation(true),
)

// AddClickHouseTools registers all ClickHouse tools with the MCP server
func AddClickHouseTools(mcp *server.MCPServer) {
	QueryClickHouseLogs.Register(mcp)
	ListClickHouseTables.Register(mcp)
	DescribeClickHouseTable.Register(mcp)
}
