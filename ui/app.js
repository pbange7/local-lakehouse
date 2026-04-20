/* ============================================================
   Local Lakehouse — Alpine.js application
   Communicates with the FastAPI backend via /api/ (proxied by nginx).
   ============================================================ */

function lakehouseApp() {
  return {
    // ── Query state ──────────────────────────────────────────
    engine:   'duckdb',
    sql:      'SELECT method, status_code, count(*) AS cnt, round(avg(latency_ms), 1) AS avg_ms\nFROM web_logs.web_logs\nGROUP BY method, status_code\nORDER BY cnt DESC\nLIMIT 20',
    loading:  false,
    error:    null,
    result:   null,
    page:     0,
    pageSize: 100,

    // ── Schema browser ───────────────────────────────────────
    schemas:       {},   // { duckdb: [{catalog, schema, table, fqn}, ...], trino: [...] }
    schemaLoading: false,
    expandedNodes: new Set(),

    // ────────────────────────────────────────────────────────
    // Initialise: load schema browser
    // ────────────────────────────────────────────────────────
    async init() {
      this.schemaLoading = true;
      try {
        const [duckdbTables, trinoTables] = await Promise.all([
          fetch('/api/tables/duckdb').then(r => r.json()),
          fetch('/api/tables/trino').then(r => r.json()),
        ]);
        this.schemas = { duckdb: duckdbTables, trino: trinoTables };
        // Auto-expand everything
        this.expandedNodes.add('duckdb');
        this.expandedNodes.add('trino');
        for (const tbl of duckdbTables) this.expandedNodes.add('duckdb.' + tbl.schema);
        for (const tbl of trinoTables)  this.expandedNodes.add('trino.'  + tbl.catalog + '.' + tbl.schema);
      } catch (e) {
        console.error('Schema load failed:', e);
      } finally {
        this.schemaLoading = false;
      }
    },

    // ────────────────────────────────────────────────────────
    // Execute SQL query
    // ────────────────────────────────────────────────────────
    async runQuery() {
      if (this.loading || !this.sql.trim()) return;
      this.loading = true;
      this.error   = null;
      this.result  = null;
      this.page    = 0;

      try {
        const resp = await fetch(`/api/query/${this.engine}`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ sql: this.sql, limit: 10000 }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          this.error = data.detail ?? `HTTP ${resp.status}`;
        } else {
          this.result = data;
        }
      } catch (e) {
        this.error = 'Network error: ' + e.message;
      } finally {
        this.loading = false;
      }
    },

    // ────────────────────────────────────────────────────────
    // Computed: current page rows
    // ────────────────────────────────────────────────────────
    get displayRows() {
      if (!this.result) return [];
      const start = this.page * this.pageSize;
      return this.result.rows.slice(start, start + this.pageSize);
    },

    get totalPages() {
      if (!this.result) return 0;
      return Math.max(1, Math.ceil(this.result.rows.length / this.pageSize));
    },

    // ────────────────────────────────────────────────────────
    // Schema browser helpers
    // ────────────────────────────────────────────────────────
    toggleNode(key) {
      if (this.expandedNodes.has(key)) {
        this.expandedNodes.delete(key);
      } else {
        this.expandedNodes.add(key);
      }
      // Alpine doesn't track Set mutations — force reactivity
      this.expandedNodes = new Set(this.expandedNodes);
    },

    groupBySchema(tables) {
      const map = {};
      for (const tbl of tables) {
        const key = tbl.catalog ? `${tbl.catalog}.${tbl.schema}` : tbl.schema;
        if (!map[key]) map[key] = { name: key, tables: [] };
        map[key].tables.push(tbl);
      }
      return Object.values(map);
    },

    insertTable(fqn) {
      this.sql = `SELECT *\nFROM ${fqn}\nLIMIT 100`;
      // Auto-run on click
      this.$nextTick(() => this.runQuery());
    },

    // ────────────────────────────────────────────────────────
    // CSV download
    // ────────────────────────────────────────────────────────
    downloadCsv() {
      if (!this.result) return;
      const escape = v => {
        if (v === null || v === undefined) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n')
          ? '"' + s.replace(/"/g, '""') + '"'
          : s;
      };
      const lines = [
        this.result.columns.map(escape).join(','),
        ...this.result.rows.map(row => row.map(escape).join(',')),
      ];
      const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = 'results.csv';
      a.click();
      URL.revokeObjectURL(url);
    },
  };
}
