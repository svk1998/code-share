parseTSV(raw) {
  const rows = []
  let i = 0

  while (i < raw.length) {
    const { cells, nextIndex } = this.parseRow(raw, i)
    if (cells.length > 0) rows.push(cells)
    if (nextIndex === i) break   // safety guard
    i = nextIndex
  }

  if (rows.length < 2) {
    this.showSnackbar('error', 'Need at least a header row and one data row.')
    return
  }

  this.headers = rows[0]
  this.rows = rows.slice(1).map(cells => {
    while (cells.length < this.headers.length) cells.push('')
    return cells
  })

  this.$emit('parsed', { headers: this.headers, rows: this.rows })
},

// Parses one row from position `start`, returns cells + next row start index
parseRow(str, start) {
  const cells = []
  let cell = ''
  let inQuotes = false
  let i = start

  while (i < str.length) {
    const ch = str[i]

    if (inQuotes) {
      if (ch === '"' && str[i + 1] === '"') {
        // Escaped quote inside cell → ""
        cell += '"'
        i += 2
      } else if (ch === '"') {
        // Closing quote
        inQuotes = false
        i++
      } else {
        // \r\n INSIDE quotes = part of cell content, not a row break
        cell += ch
        i++
      }
    } else {
      if (ch === '"') {
        inQuotes = true
        i++
      } else if (ch === '\t') {
        cells.push(cell.trim())
        cell = ''
        i++
      } else if (ch === '\r' && str[i + 1] === '\n') {
        // \r\n OUTSIDE quotes = actual row break
        cells.push(cell.trim())
        return { cells, nextIndex: i + 2 }
      } else if (ch === '\n') {
        cells.push(cell.trim())
        return { cells, nextIndex: i + 1 }
      } else {
        cell += ch
        i++
      }
    }
  }

  // Last row (no trailing newline)
  if (cell || cells.length > 0) cells.push(cell.trim())
  return { cells, nextIndex: i }
},
