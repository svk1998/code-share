<template>
  <div class="excel-paste-import">

    <!-- Steps -->
    <div class="epi-steps">
      <div
        v-for="(label, i) in stepLabels"
        :key="i"
        class="epi-step"
        :class="{ active: step >= i + 1, done: step > i + 1 }"
      >
        <div class="epi-step-num">
          <v-icon v-if="step > i + 1" small color="#00e5a0">mdi-check</v-icon>
          <span v-else>{{ i + 1 }}</span>
        </div>
        <div class="epi-step-label">{{ label }}</div>
      </div>
    </div>

    <!-- Paste Zone -->
    <div
      v-if="!hasData"
      class="epi-paste-zone"
      :class="{ focused: isFocused }"
      tabindex="0"
      @click="focusPasteArea"
      @focus="isFocused = true"
      @blur="isFocused = false"
    >
      <textarea
        ref="hiddenInput"
        class="epi-hidden-input"
        @paste="handlePaste"
        @focus="isFocused = true"
        @blur="isFocused = false"
      />
      <div class="epi-paste-icon">
        <v-icon size="28" color="#00e5a0">mdi-table-arrow-down</v-icon>
      </div>
      <div class="epi-paste-title">Click here, then paste your Excel data</div>
      <div class="epi-paste-hint">
        <kbd>Ctrl+A</kbd> in Excel &nbsp;→&nbsp;
        <kbd>Ctrl+C</kbd> &nbsp;→&nbsp; click here &nbsp;→&nbsp;
        <kbd>Ctrl+V</kbd>
      </div>
    </div>

    <!-- Data Preview -->
    <div v-if="hasData">
      <div class="epi-stats-bar">
        <div class="epi-chip"><span>{{ headers.length }}</span>&nbsp;columns</div>
        <div class="epi-chip"><span>{{ totalRows }}</span>&nbsp;rows</div>
        <div class="epi-chip"><span>{{ emptyCount }}</span>&nbsp;empty cells</div>
        <v-spacer />
        <button class="epi-clear-btn" @click="clearData">
          <v-icon small color="#ff4d6d">mdi-close</v-icon> Clear
        </button>
      </div>

      <div v-if="totalRows > previewLimit" class="epi-limit-notice">
        Showing first <span>{{ previewLimit }}</span> of {{ totalRows }} rows
      </div>

      <div class="epi-table-wrap">
        <div class="epi-table-scroll">
          <table class="epi-table">
            <thead>
              <tr>
                <th class="epi-row-num">#</th>
                <th v-for="(h, i) in headers" :key="i">
                  <span class="epi-col-letter">{{ colLetter(i) }}</span>
                  {{ h || '(empty)' }}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, ri) in previewRows" :key="ri">
                <td class="epi-row-num">{{ ri + 1 }}</td>
                <td
                  v-for="(cell, ci) in row"
                  :key="ci"
                  :class="{ 'epi-empty': !cell && cell !== 0 }"
                >
                  {{ (cell !== '' && cell !== null && cell !== undefined) ? cell : '—' }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <v-btn
        block depressed
        :loading="sending"
        :disabled="sending || !hasData"
        class="epi-send-btn"
        @click="sendToBackend"
      >
        <v-icon left>mdi-send</v-icon>
        Send to Backend
      </v-btn>
    </div>

    <v-snackbar
      v-model="snackbar.show"
      :color="snackbar.type === 'success' ? '#00e5a0' : '#ff4d6d'"
      :timeout="3500"
      bottom right
    >
      <span :style="{ color: '#000', fontWeight: 600 }">{{ snackbar.message }}</span>
    </v-snackbar>

  </div>
</template>

<script>
export default {
  name: 'ExcelPasteImport',

  props: {
    endpoint:     { type: String, default: '/api/import/excel' },
    previewLimit: { type: Number, default: 50 }
  },

  data() {
    return {
      step: 1,
      isFocused: false,
      headers: [],
      rows: [],
      sending: false,
      stepLabels: [
        'Open in Excel & Select All',
        'Ctrl+C to Copy',
        'Paste Here',
        'Send to Backend'
      ],
      snackbar: { show: false, type: 'success', message: '' }
    }
  },

  computed: {
    hasData()    { return this.rows.length > 0 },
    previewRows(){ return this.rows.slice(0, this.previewLimit) },
    totalRows()  { return this.rows.length },
    emptyCount() {
      return this.rows.reduce((acc, row) =>
        acc + row.filter(c => c === '' || c === null || c === undefined).length, 0)
    }
  },

  watch: {
    hasData(val) { this.step = val ? 4 : 1 }
  },

  methods: {
    colLetter(i) { return String.fromCharCode(65 + (i % 26)) },

    focusPasteArea() {
      this.$refs.hiddenInput.focus()
      this.step = 3
    },

    handlePaste(e) {
      const tsv = e.clipboardData.getData('text/plain')
      if (!tsv?.trim()) {
        this.showSnackbar('error', 'Nothing to paste. Copy from Excel first.')
        return
      }
      this.parseTSV(tsv)
    },

    parseTSV(raw) {
      const lines = raw.trim().split(/\r?\n/)
      if (lines.length < 2) {
        this.showSnackbar('error', 'Need at least a header row and one data row.')
        return
      }
      this.headers = lines[0].split('\t')
      this.rows = lines.slice(1).map(line => {
        const cells = line.split('\t')
        while (cells.length < this.headers.length) cells.push('')
        return cells
      })
      this.$emit('parsed', { headers: this.headers, rows: this.rows })
    },

    async sendToBackend() {
      this.sending = true
      try {
        const payload = {
          headers: this.headers,
          rows: this.rows,
          rowCount: this.totalRows,
          timestamp: new Date().toISOString()
        }
        const res = await fetch(this.endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const responseData = await res.json().catch(() => ({}))
        this.showSnackbar('success', `${this.totalRows} rows sent successfully.`)
        this.$emit('success', { payload, response: responseData })
      } catch (err) {
        this.showSnackbar('error', `Send failed: ${err.message}`)
        this.$emit('error', err)
      } finally {
        this.sending = false
      }
    },

    clearData() {
      this.headers = []
      this.rows = []
      this.step = 1
      this.$emit('cleared')
      this.$nextTick(() => this.focusPasteArea())
    },

    showSnackbar(type, message) {
      this.snackbar = { show: true, type, message }
    }
  }
}
</script>
<style scoped> 
   .excel-paste-import { font-family: 'JetBrains Mono', monospace; color: #e8eaf0; }

/* Steps */
.epi-steps { display: flex; border: 1px solid #252a38; border-radius: 8px; overflow: hidden; margin-bottom: 20px; }
.epi-step { flex: 1; display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: #13161e; border-right: 1px solid #252a38; transition: background 0.2s; }
.epi-step:last-child { border-right: none; }
.epi-step.active, .epi-step.done { background: #1a1e2a; }
.epi-step-num { width: 22px; height: 22px; border-radius: 50%; border: 1.5px solid #4a5068; display: flex; align-items: center; justify-content: center; font-size: 11px; flex-shrink: 0; transition: all 0.2s; }
.epi-step.active .epi-step-num { border-color: #00e5a0; color: #00e5a0; }
.epi-step.done   .epi-step-num { border-color: #00e5a0; background: #00e5a0; color: #000; }
.epi-step-label { font-size: 11px; font-weight: 600; color: #4a5068; }
.epi-step.active .epi-step-label, .epi-step.done .epi-step-label { color: #e8eaf0; }

/* Paste zone */
.epi-paste-zone { border: 1.5px dashed #252a38; border-radius: 10px; background: #13161e; min-height: 200px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; cursor: pointer; transition: all 0.2s; position: relative; overflow: hidden; }
.epi-paste-zone:focus { outline: none; }
.epi-paste-zone.focused { border-color: #00e5a0; background: rgba(0,229,160,0.04); }
.epi-hidden-input { position: absolute; opacity: 0; width: 1px; height: 1px; pointer-events: none; }
.epi-paste-icon { width: 52px; height: 52px; border-radius: 12px; background: #1a1e2a; border: 1px solid #252a38; display: flex; align-items: center; justify-content: center; }
.epi-paste-title { font-size: 16px; font-weight: 700; font-family: sans-serif; color: #e8eaf0; }
.epi-paste-hint { font-size: 12px; color: #4a5068; background: #1a1e2a; border: 1px solid #252a38; padding: 4px 12px; border-radius: 4px; }
.epi-paste-hint kbd { background: #252a38; color: #e8eaf0; padding: 2px 6px; border-radius: 3px; font-size: 11px; }

/* Stats */
.epi-stats-bar { display: flex; align-items: center; gap: 6px; margin: 14px 0 8px; flex-wrap: wrap; }
.epi-chip { background: #1a1e2a; border: 1px solid #252a38; padding: 3px 10px; border-radius: 4px; font-size: 12px; color: #4a5068; }
.epi-chip span { color: #00e5a0; font-weight: 600; }
.epi-clear-btn { background: transparent; border: 1px solid #252a38; color: #ff4d6d; padding: 3px 10px; border-radius: 4px; font-size: 12px; cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.15s; font-family: 'JetBrains Mono', monospace; }
.epi-clear-btn:hover { background: rgba(255,77,109,0.1); border-color: #ff4d6d; }
.epi-limit-notice { font-size: 11px; color: #4a5068; text-align: right; margin-bottom: 6px; }
.epi-limit-notice span { color: #f0a500; }

/* Table */
.epi-table-wrap { border: 1px solid #252a38; border-radius: 10px; overflow: hidden; margin-bottom: 16px; }
.epi-table-scroll { overflow-x: auto; max-height: 340px; overflow-y: auto; }
.epi-table-scroll::-webkit-scrollbar { width: 5px; height: 5px; }
.epi-table-scroll::-webkit-scrollbar-track { background: #13161e; }
.epi-table-scroll::-webkit-scrollbar-thumb { background: #252a38; border-radius: 3px; }
.epi-table { width: 100%; border-collapse: collapse; }
.epi-table thead tr { background: #1a1e2a; position: sticky; top: 0; z-index: 2; }
.epi-table thead th { padding: 9px 13px; text-align: left; font-size: 11px; font-weight: 600; color: #00e5a0; border-bottom: 1px solid #252a38; white-space: nowrap; }
.epi-col-letter { color: #4a5068; font-size: 9px; margin-right: 3px; }
.epi-table tbody tr { border-bottom: 1px solid #252a38; transition: background 0.1s; }
.epi-table tbody tr:last-child { border-bottom: none; }
.epi-table tbody tr:hover { background: #1a1e2a; }
.epi-table tbody td { padding: 8px 13px; font-size: 12px; white-space: nowrap; max-width: 200px; overflow: hidden; text-overflow: ellipsis; color: #e8eaf0; }
.epi-row-num { color: #4a5068; font-size: 10px; text-align: center; min-width: 30px; }
.epi-empty { color: #4a5068; font-style: italic; }

/* Send button */
.epi-send-btn { background: #00e5a0 !important; color: #000 !important; font-weight: 700 !important; font-size: 14px !important; height: 46px !important; border-radius: 8px !important; }
.epi-send-btn:hover { background: #00ffb3 !important; }

</style>
