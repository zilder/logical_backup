package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

type (
	MType           int
	ReplicaIdentity uint8
	TupleKind       uint8
)

const (
	ReplicaIdentityDefault ReplicaIdentity = 'd'
	ReplicaIdentityNothing                 = 'n'
	ReplicaIdentityIndex                   = 'i'
	ReplicaIdentityFull                    = 'f'

	TupleNull      TupleKind = 'n' // Identifies the data as NULL value.
	TupleUnchanged           = 'u' // Identifies unchanged TOASTed value (the actual value is not sent).
	TupleText                = 't' // Identifies the data as text formatted value.

	MsgInsert MType = iota
	MsgUpdate
	MsgDelete
	MsgBegin
	MsgCommit
	MsgRelation
	MsgType
	MsgOrigin
	MsgTruncate
)

var (
	replicaIdentities = map[ReplicaIdentity]string{
		ReplicaIdentityDefault: "default",
		ReplicaIdentityIndex:   "index",
		ReplicaIdentityNothing: "nothing",
		ReplicaIdentityFull:    "full",
	}

	typeNames = map[MType]string{
		MsgBegin:    "begin",
		MsgRelation: "relation",
		MsgUpdate:   "update",
		MsgInsert:   "insert",
		MsgDelete:   "delete",
		MsgCommit:   "commit",
		MsgOrigin:   "origin",
		MsgType:     "type",
		MsgTruncate: "truncate",
	}
)

type DumpInfo struct {
	StartLSN       dbutils.LSN   `yaml:"StartLSN"`
	CreateDate     time.Time     `yaml:"CreateDate"`
	Relation       Relation      `yaml:"Relation"`
	BackupDuration time.Duration `yaml:"BackupDuration"`
}

type Message interface {
	fmt.Stringer

	MsgType() MType
	RawData() []byte
}

type RawMessage struct {
	Message
	Data []byte
}

func (m RawMessage) RawData() []byte {
	return m.Data
}

type NamespacedName struct {
	Namespace string `yaml:"Namespace"`
	Name      string `yaml:"Name"`
}

type Column struct {
	IsKey   bool        `yaml:"IsKey"` // column as part of the key.
	Name    string      `yaml:"Name"`  // Name of the column.
	TypeOID dbutils.OID `yaml:"OID"`   // OID of the column's data type.
	Mode    int32       `yaml:"Mode"`  // OID modifier of the column (atttypmod).
}

type TupleData struct {
	Kind  TupleKind
	Value []byte
}

type Begin struct {
	RawMessage
	FinalLSN  dbutils.LSN // LSN of the record that lead to this xact to be committed
	Timestamp time.Time   // Commit timestamp of the transaction
	XID       int32       // Xid of the transaction.
}

type Commit struct {
	RawMessage
	Flags          uint8       // Flags; currently unused (must be 0)
	LSN            dbutils.LSN // The LastLSN of the commit.
	TransactionLSN dbutils.LSN // LSN pointing to the end of the commit record + 1
	Timestamp      time.Time   // Commit timestamp of the transaction
}

type Origin struct {
	RawMessage
	LSN  dbutils.LSN // The last LSN of the commit on the origin server.
	Name string
}

type Relation struct {
	RawMessage
	NamespacedName `yaml:"NamespacedName"`

	OID             dbutils.OID     `yaml:"OID"`             // OID of the relation.
	ReplicaIdentity ReplicaIdentity `yaml:"ReplicaIdentity"` // Replica identity
	Columns         []Column        `yaml:"Columns"`         // Columns
}

type Insert struct {
	RawMessage
	RelationOID dbutils.OID // OID of the relation corresponding to the OID in the relation message.

	NewRow []TupleData
}

type Update struct {
	RawMessage
	RelationOID dbutils.OID // OID of the relation corresponding to the OID in the relation message.

	OldRow []TupleData
	NewRow []TupleData
}

type Delete struct {
	RawMessage
	RelationOID dbutils.OID // OID of the relation corresponding to the OID in the relation message.

	OldRow []TupleData
}

type Truncate struct {
	RawMessage
	Cascade         bool
	RestartIdentity bool
	RelationOIDs    []dbutils.OID
}

type Type struct {
	RawMessage
	NamespacedName

	OID dbutils.OID // OID of the data type
}

type queryRunner interface {
	QueryRow(sql string, args ...interface{}) *pgx.Row
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
}

func (t MType) String() string {
	str, ok := typeNames[t]
	if !ok {
		return "unknown"
	}

	return str
}

func (Begin) MsgType() MType    { return MsgBegin }
func (Relation) MsgType() MType { return MsgRelation }
func (Update) MsgType() MType   { return MsgUpdate }
func (Insert) MsgType() MType   { return MsgInsert }
func (Delete) MsgType() MType   { return MsgDelete }
func (Commit) MsgType() MType   { return MsgCommit }
func (Origin) MsgType() MType   { return MsgOrigin }
func (Type) MsgType() MType     { return MsgType }
func (Truncate) MsgType() MType { return MsgTruncate }

func (t TupleData) String() string {
	switch t.Kind {
	case TupleText:
		return dbutils.QuoteLiteral(string(t.Value))
	case TupleNull:
		return "null"
	case TupleUnchanged:
		return "[unchanged value]"
	default:
		return "unknown"
	}
}

func (t TupleData) IsNull() bool { return t.Kind == TupleNull }
func (t TupleData) IsText() bool { return t.Kind == TupleText }

func (m Begin) String() string {
	return fmt.Sprintf("FinalLSN:%s Timestamp:%v XID:%d",
		m.FinalLSN.String(), m.Timestamp.Format(time.RFC3339), m.XID)
}

func (m Relation) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("OID:%s", m.OID))
	parts = append(parts, fmt.Sprintf("Name:%s", m.NamespacedName))
	parts = append(parts, fmt.Sprintf("RepIdentity:%s", m.ReplicaIdentity))

	columns := make([]string, 0)
	for _, c := range m.Columns {
		var isKey, mode string

		if c.IsKey {
			isKey = " key"
		}

		if c.Mode != -1 {
			mode = fmt.Sprintf(" atttypmod:%v", c.Mode)
		}
		colStr := fmt.Sprintf("%q (type:%s)%s%s", c.Name, c.TypeOID, isKey, mode)
		columns = append(columns, colStr)
	}

	parts = append(parts, fmt.Sprintf("Columns:[%s]", strings.Join(columns, ", ")))

	return strings.Join(parts, " ")
}

// Returns `delimiter` separated list of TupleData array as a string
func joinTupleData(values []TupleData, delimiter string) string {
	var b strings.Builder
	first := true

	for _, v := range values {
		if !first {
			b.WriteString(delimiter)
		} else {
			first = false
		}

		b.WriteString(v.String())
	}
	return b.String()
}

func (m Update) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.NewRow != nil {
		parts = append(parts, fmt.Sprintf("newValues: [%s]", joinTupleData(m.NewRow, ", ")))
	}

	if m.OldRow != nil {
		parts = append(parts, fmt.Sprintf("oldValues: [%s]", joinTupleData(m.OldRow, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Insert) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))
	if m.NewRow != nil {
		parts = append(parts, fmt.Sprintf("newValues: [%s]", joinTupleData(m.NewRow, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Delete) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))
	if m.OldRow != nil {
		parts = append(parts, fmt.Sprintf("oldValues: [%s]", joinTupleData(m.OldRow, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Commit) String() string {
	return fmt.Sprintf("LSN:%s Timestamp:%v TxEndLSN:%s",
		m.LSN, m.Timestamp.Format(time.RFC3339), m.TransactionLSN)
}

func (m Origin) String() string {
	return fmt.Sprintf("LSN:%s Name:%s", m.LSN, m.Name)
}

func (m Type) String() string {
	return fmt.Sprintf("OID:%s Name:%s", m.OID, m.NamespacedName)
}

func (m Truncate) String() string {
	parts := make([]string, 0)
	oids := make([]string, 0)

	if m.Cascade {
		parts = append(parts, "cascade")
	}

	if m.RestartIdentity {
		parts = append(parts, "restart_identity")
	}

	for _, oid := range m.RelationOIDs {
		oids = append(oids, oid.String())
	}

	if len(oids) > 0 {
		parts = append(parts, fmt.Sprintf("tableOids:[%s]", strings.Join(oids, ", ")))
	}

	return strings.Join(parts, " ")
}

func (tr Truncate) SQL() string {
	//TODO
	return ""
}

func (ins Insert) SQL(rel Relation) string {
	values := make([]string, 0)
	names := make([]string, 0)
	for i, v := range rel.Columns {
		names = append(names, pgx.Identifier{v.Name}.Sanitize())
		val := ins.NewRow[i]
		if val.IsText() || val.IsNull() {
			values = append(values, ins.NewRow[i].String())
		}
	}

	return fmt.Sprintf("insert into %s (%s) values (%s);",
		rel.Sanitize(),
		strings.Join(names, ", "),
		strings.Join(values, ", "))
}

func (upd Update) SQL(rel Relation) string {
	values := make([]string, 0)
	cond := make([]string, 0)

	for i, v := range rel.Columns {
		colName := pgx.Identifier{string(v.Name)}.Sanitize()
		newVal := upd.NewRow[i]

		if newVal.IsText() || newVal.IsNull() {
			values = append(values, fmt.Sprintf("%s = %s", colName, newVal.String()))
		}

		if upd.OldRow != nil {
			oldVal := upd.OldRow[i]

			if oldVal.IsText() {
				cond = append(cond, fmt.Sprintf("%s = %s", colName, oldVal.String()))
			} else if oldVal.IsNull() {
				cond = append(cond, fmt.Sprintf("%s is null", colName))
			}
		} else {
			// If there is no old row it means that the key defined by REPLICA
			// IDENTITY didn't change and we can use values from new row
			// to build a conditions string
			if v.IsKey {
				if newVal.IsText() {
					cond = append(cond, fmt.Sprintf("%s = %s", colName, newVal.String()))
				} else if newVal.IsNull() {
					cond = append(cond, fmt.Sprintf("%s is null", colName))
				}
			}
		}
	}

	sql := fmt.Sprintf("update %s set %s", rel.Sanitize(), strings.Join(values, ", "))
	if len(cond) > 0 {
		sql += " where " + strings.Join(cond, " and ")
	}
	sql += ";"

	return sql
}

func (del Delete) SQL(rel Relation) string {
	cond := make([]string, 0)
	for i, v := range rel.Columns {
		colName := pgx.Identifier{string(v.Name)}.Sanitize()
		val := del.OldRow[i]

		if val.IsText() {
			cond = append(cond, fmt.Sprintf("%s = %s", colName, val.String()))
		} else if val.IsNull() {
			cond = append(cond, fmt.Sprintf("%s is null", colName))
		}
	}

	return fmt.Sprintf("delete from %s where %s;", rel.Sanitize(), strings.Join(cond, " and "))
}

func (rel Relation) SQL(oldRel Relation) string {
	sqlCommands := make([]string, 0)

	quotedTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()
	oldTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()

	if oldTableName != quotedTableName {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s rename to %s", oldTableName, quotedTableName))
	}

	newColumns := make([]Column, 0)
	deletedColumns := make(map[string]Column)
	alteredColumns := make(map[Column]Column)
	for _, c := range oldRel.Columns {
		deletedColumns[c.Name] = c
	}

	for id, col := range rel.Columns {
		oldCol, ok := deletedColumns[col.Name]
		if !ok {
			newColumns = append(newColumns, col)
			break
		}
		if oldCol.TypeOID != col.TypeOID || oldCol.Mode != col.Mode {
			alteredColumns[oldCol] = rel.Columns[id]
		}

		delete(deletedColumns, col.Name)
	}

	for _, col := range deletedColumns {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s drop column %s;", quotedTableName, pgx.Identifier{col.Name}.Sanitize()))
	}

	for _, col := range newColumns {
		typMod := "NULL"
		if col.Mode > 0 {
			typMod = fmt.Sprintf("%d", col.Mode)
		}
		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s add column %s ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{col.Name}.Sanitize(), col.TypeOID, typMod))
	}

	for oldCol, newCol := range alteredColumns {
		typMod := "NULL"
		if newCol.Mode > 0 {
			typMod = fmt.Sprintf("%d", newCol.Mode)
		}

		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s alter column %s type ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{oldCol.Name}.Sanitize(), newCol.TypeOID, typMod))
	}

	if oldRel.ReplicaIdentity != rel.ReplicaIdentity {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s replica identity %s;", quotedTableName, replicaIdentities[rel.ReplicaIdentity]))
	}

	return strings.Join(sqlCommands, " ")
}

func (r ReplicaIdentity) String() string {
	if name, ok := replicaIdentities[r]; !ok {
		return replicaIdentities[ReplicaIdentityDefault]
	} else {
		return name
	}
}

func (r ReplicaIdentity) MarshalYAML() (interface{}, error) {
	return replicaIdentities[r], nil
}

func (r *ReplicaIdentity) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	for k, v := range replicaIdentities {
		if val == v {
			*r = k
			return nil
		}
	}

	return fmt.Errorf("unknown replica identity: %q", val)
}

func (t TupleKind) String() string {
	switch t {
	case TupleNull:
		return "null"
	case TupleUnchanged:
		return "unchanged"
	case TupleText:
		return "text"
	default:
		return "unknown"
	}
}

func (n NamespacedName) String() string {
	if n.Namespace == "public" {
		return n.Name
	}

	return strings.Join([]string{n.Namespace, n.Name}, ".")
}

func (n NamespacedName) Sanitize() string {
	return pgx.Identifier{n.Namespace, n.Name}.Sanitize()
}

func (rel Relation) Structure() string {
	result := fmt.Sprintf("%s (OID: %v)", rel.NamespacedName, rel.OID)

	cols := make([]string, 0)
	for _, c := range rel.Columns {
		cols = append(cols, fmt.Sprintf("%s(%v)", c.Name, c.TypeOID))
	}

	if len(cols) > 0 {
		result += fmt.Sprintf(" Columns: %s", strings.Join(cols, ", "))
	}

	return result
}

func (rel *Relation) fetchColumns(conn queryRunner) error {
	var columns []Column

	if rel.OID == dbutils.InvalidOID {
		return fmt.Errorf("table has no oid")
	}

	// query taken from fetch_remote_table_info (src/backend/replication/logical/tablesync.c)
	query := fmt.Sprintf(`
	  SELECT a.attname,
	       a.atttypid,
	       a.atttypmod,
	       coalesce(a.attnum = ANY(i.indkey), false)
	  FROM pg_catalog.pg_attribute a
	  LEFT JOIN pg_catalog.pg_index i
	       ON (i.indexrelid = pg_get_replica_identity_index(%[1]d))
	  WHERE a.attnum > 0::pg_catalog.int2
	  AND NOT a.attisdropped
	  AND a.attrelid = %[1]d
      ORDER BY a.attnum`, rel.OID)

	rows, err := conn.Query(query)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var column Column

		if err := rows.Scan(&column.Name, &column.TypeOID, &column.Mode, &column.IsKey); err != nil {
			return fmt.Errorf("could not scan: %v", err)
		}

		columns = append(columns, column)
	}

	rel.Columns = columns

	return nil
}

// FetchByOID fetches relation info from the database by oid
func (rel *Relation) FetchByOID(conn queryRunner, oid dbutils.OID) error {
	var relreplident, tableName, schemaName string

	row := conn.QueryRow(fmt.Sprintf(`select relnamespace::regnamespace::text, relname, relreplident::text from pg_class where oid = %d`, oid))
	if err := row.Scan(&schemaName, &tableName, &relreplident); err != nil {
		return fmt.Errorf("could not fetch replica identity: %v", err)
	}
	rel.Name = tableName
	rel.Namespace = schemaName
	rel.ReplicaIdentity = ReplicaIdentity(relreplident[0])
	rel.OID = oid

	if err := rel.fetchColumns(conn); err != nil {
		return fmt.Errorf("could not fetch columns: %v", err)
	}

	return nil
}

// FetchByName fetches relation info from the database by name
func (rel *Relation) FetchByName(conn queryRunner, tableName NamespacedName) error {
	var (
		relreplident string
		oid          dbutils.OID
	)

	row := conn.QueryRow(fmt.Sprintf(`select oid, relreplident::text from pg_class where oid = '%s'::regclass::oid`, tableName.Sanitize()))
	if err := row.Scan(&oid, &relreplident); err != nil {
		return fmt.Errorf("could not fetch replica identity: %v", err)
	}
	rel.NamespacedName = tableName
	rel.ReplicaIdentity = ReplicaIdentity(relreplident[0])
	rel.OID = oid

	if err := rel.fetchColumns(conn); err != nil {
		return fmt.Errorf("could not fetch columns: %v", err)
	}

	return nil
}

// Equals checks if rel2 is the same as rel. except for Raw field
func (rel *Relation) Equals(rel2 *Relation) bool {
	if rel.OID != rel2.OID {
		return false
	}

	if rel.NamespacedName != rel2.NamespacedName {
		return false
	}

	if rel.ReplicaIdentity != rel2.ReplicaIdentity {
		return false
	}

	if len(rel.Columns) != len(rel2.Columns) {
		return false
	}

	for i := range rel.Columns {
		if rel.Columns[i] != rel2.Columns[i] {
			return false
		}
	}

	return true
}
