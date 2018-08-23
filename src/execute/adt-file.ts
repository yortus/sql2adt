/* Derived from code from https://github.com/albertzak/node_adt, with the following license:
 *     Copyright (c) 2010-2010 Chase Gray mailto:chase@ratchetsoftware.com
 *     Copyright (c) 2015 Albert Zak mailto:me@albertzak.com
 *     Copyright (c) 2018 Troy Gerwien mailto:yortus@gmail.com
 *
 *     Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 *     documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 *     the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 *     to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 *     The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 *     the Software.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 *     THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 *     CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *     IN THE SOFTWARE.
 */
import * as fs from 'fs';
import {promisify} from 'util';




/** Represents an ADT file. */
export class AdtFile {

    /** Opens the ADT file at the specified path. */
    static async open(path: string) {
        let fd = await fsOpen(path, 'r');
        let header = await parseHeader(fd);
        let columns = await parseColumns(fd, header);
        return new AdtFile(fd, header, columns);
    }

    /**
     * The number of records in the ADT file. This number is inclusive of records marked as
     * deleted, so it may be greater than the total number of records returned by fetchRecords.
     */
    recordCount: number;

    /** The names of all the columns in the ADT file. */
    columnNames: string[];

    /**
     * Reads and returns records from the ADT file.
     * NB1: no index file is consulted, so records are returned in natural (i.e., file) order.
     * NB2: deleted records are skipped, so the number of records returned may be less than limit / recordCount.
     * @param options.offset number of records to skip before reading records. Default: 0
     * @param options.limit number of records to read before returning. Default: <record count - offset>
     * @param options.columnNames subset of column names to return (case-insensitive). Default: <all column names>
     */
     async fetchRecords(options?: {offset?: number, limit?: number, columnNames?: string[]}) {
        options = options || {};
        let header = this.header;

        // Calculate iteration limits. Ensure they are clamped within a valid range.
        let startingIndex = typeof options.offset === 'number' ? options.offset : 0;
        if (startingIndex < 0) startingIndex = 0;
        if (startingIndex > header.recordCount) startingIndex = header.recordCount;
        let finishedIndex =  typeof options.limit === 'number' ? startingIndex + options.limit : header.recordCount;
        if (finishedIndex < startingIndex) finishedIndex = startingIndex;
        if (finishedIndex > header.recordCount) finishedIndex = header.recordCount;

        // Calculate column name whitelist. Compare options.columnNames case-insensitively.
        let columns = this.columns;
        if (options.columnNames) {
            let whitelist = options.columnNames;
            let invalidNames = whitelist.filter(wn => !columns.some(c => c.name.toLowerCase() === wn.toLowerCase()));
            if (invalidNames.length > 0) throw new Error(`Invalid column name(s): ${invalidNames}`);
            columns = whitelist.map(wn => columns.find(col => col.name.toLowerCase() === wn.toLowerCase())!);
        }

        // Read all fetched records into a single buffer.
        // TODO: use a chunked approach if file is *very* large? Probably not, largest we've seen are ~50MB.
        let buffer = Buffer.alloc((finishedIndex - startingIndex) * header.recordLength);
        await fsRead(this.fd, buffer, 0, buffer.length, header.dataOffset + startingIndex * header.recordLength);

        // Parse each record out of the buffer into an object
        let parseRecord = makeRecordParser(columns);
        let records = [] as Record[];
        for (let recordOffset = 0; recordOffset < buffer.length; recordOffset += header.recordLength) {
            // Skip records marked as deleted. When this happens, less than `limit` records will be returned.
            if (buffer.readInt8(recordOffset) === 0x05)  continue; // first byte of 0x05 indicates record is deleted
            let record = parseRecord(buffer, recordOffset);
            records.push(record);
        }
        return records;
    }

    /** Closes the ADT file. Call this as part of cleanup to prevent leaking file descriptors. */
    async close() {
        if (this.fd !== -1) {
            let closed = fsClose(this.fd);
            this.fd = -1;
            await closed;
        }
    };

    /** Creates an AdtFile instance. Not to be called directly (hence private). Use AdtFile.open(). */
    private constructor(private fd: number, private header: Header, private columns: Column[]) {
        this.recordCount = header.recordCount;
        this.columnNames = columns.map(c => c.name);
    }
}




/** Retrieves the record count, column count, record length, and data offset for an ADT file. */
async function parseHeader(fd: number): Promise<Header> {
    let {buffer} = await fsRead(fd, new Buffer(HEADER_LENGTH), 0, HEADER_LENGTH, 0);
    return {
        recordCount: buffer.readUInt32LE(24),
        dataOffset: buffer.readUInt32LE(32),
        recordLength: buffer.readUInt32LE(36),
        columnCount: (buffer.readUInt32LE(32) - HEADER_LENGTH) / COLUMN_LENGTH,
    };
}




/** Retrieves information about all columns in an ADT file. */
async function parseColumns(fd: number, header: Header) {

    // Read all column information into a buffer. Column info starts right after the header.
    let buffer = new Buffer(header.columnCount * COLUMN_LENGTH);
    await fsRead(fd, buffer, 0, buffer.length, HEADER_LENGTH);

    // The byte range for each individual record in an ADT file starts with 5 bytes of data *before* the data
    // for the first column. The data for each subsequent column follows contiguously from the previous column.
    let offset = 5;

    // Column info: 200 bytes per column
    // 0x00-0x7f: column name (128 bytes)
    // 0x81-0x82: column type (2 bytes)
    // 0x87-0x88: column length (2 bytes)
    let columns = Array(header.columnCount).fill(null).map(() => ({} as Column));
    for (let column of columns) {
        column.name = buffer.toString('latin1',  0, 0x80).replace(/\0/g, '').trim();
        column.type = buffer.readUInt16LE(0x81);
        column.length = buffer.readUInt16LE(0x87);
        column.offset = offset;

        // Advance the buffer and offset for the next column.
        buffer = buffer.slice(COLUMN_LENGTH);
        offset += column.length;
    }
    return columns;
}




/** Creates a fast record parsing function than is tailored to the given columns. */
function makeRecordParser(columns: Column[]) {
    [columns, parseField]; // Reference decls used only in evaled code, to prevent TS 'unused declaration' error.
    let source =`((buffer, offset) => ({
        ${columns.map(col => `
            ${col.name}: parseField(buffer, ${col.type}, offset + ${col.offset}, ${col.length}),
        `).join('')}
    }))`;
    return eval(source) as (buffer: Buffer, offset: number) => Record;
}




/**
 * Retrieves one field of one record from the database.
 * ref: http://devzone.advantagedatabase.com/dz/webhelp/advantage8.1/server1/adt_field_types_and_specifications.htm
 */
function parseField(buffer: Buffer, type: number, start: number, length: number) {
    switch (type) {
        case ColumnType.CHARACTER:
        case ColumnType.CICHARACTER:
            return buffer.toString('latin1', start, start + length).replace(/\0/g, '').trim();

        case ColumnType.NCHAR:
            return buffer.toString('ucs2', start, start + length).replace(/\0/g, '').trim();

        case ColumnType.DOUBLE:
            return buffer.readDoubleLE(start);

        case ColumnType.AUTOINCREMENT:
            return buffer.readUInt32LE(start);

        case ColumnType.INTEGER:
            let ival = buffer.readInt32LE(start);
            return ival === -2147483648 ? null : ival;

        case ColumnType.SHORT:
            return buffer.readInt16LE(start);

        case ColumnType.LOGICAL:
            let b = buffer.toString('latin1', start, start + length);
            return (b === 'T' || b === 't' || b === '1' || b === 'Y' || b === 'y');

        case ColumnType.DATE:
            let julian = buffer.readInt32LE(start);
            return julian === 0 ? null : new Date((julian - JULIAN_1970) * MS_PER_DAY);

        case ColumnType.TIMESTAMP:
            let julian2 = buffer.readInt32LE(start);
            let ms = buffer.readInt32LE(start + 4);
            return julian2 === 0 && ms === -1 ? null : new Date((julian2 - JULIAN_1970) * MS_PER_DAY + ms);

        // not implemented / not supported
        case ColumnType.TIME:
        default:
            return NOT_SUPPORTED;
    }
}




// Types used for parsing ADT files.
interface Header {
    recordCount: number;
    dataOffset: number;
    recordLength: number;
    columnCount: number;
}
interface Column {
    name: string;
    type: ColumnType;
    offset: number;
    length: number;
}
const enum ColumnType {
    LOGICAL       = 1,
    CHARACTER     = 4,
    CICHARACTER   = 20,
    NCHAR         = 26,
    DOUBLE        = 10,
    INTEGER       = 11,
    AUTOINCREMENT = 15,
    SHORT         = 12,
    DATE          = 3,
    TIME          = 13,
    TIMESTAMP     = 14,
}
interface Record {
    [columnName: string]: unknown;
}




// Promisified fs functions used to work with ADT files.
const fsOpen = promisify(fs.open);
const fsRead = promisify(fs.read);
const fsClose = promisify(fs.close);




// Constants used in ADT file parsing and conversions.
const HEADER_LENGTH = 400;
const COLUMN_LENGTH = 200;
const MS_PER_DAY = 1000 * 60 * 60 * 24;
const JULIAN_1970 = 2440588;
const NOT_SUPPORTED = Symbol('Unsupported datatype');
