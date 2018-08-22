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
import * as iconv from 'iconv-lite';
import {promisify} from 'util';





// TODO: added...
const fsOpen = promisify(fs.open);
const fsRead = promisify(fs.read);
const fsClose = promisify(fs.close);




const MS_PER_DAY = 1000 * 60 * 60 * 24;
const JULIAN_1970 = 2440588;




// TODO: added...
const HEADER_LENGTH = 400;
const COLUMN_LENGTH = 200;
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




// TODO: added...
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
export interface Record {
    [columnName: string]: unknown;
}




export class AdtFile {

    // TODO: jsdoc...
    static async open(path: string, encoding: string) {
        encoding = encoding || 'ISO-8859-1';
        let fd = await fsOpen(path, 'r');
        let header = await this.parseHeader(fd);
        let columns = await this.parseColumns(fd, encoding, header);
        return new AdtFile(encoding, fd, header, columns);
    }

    // TODO: jsdoc...
    async fetchRecords(options?: {offset?: number, limit?: number, columnNames?: string[]}) {
        options = options || {};

        // Calculate iteration limits
        let startingIndex = typeof options.offset === 'number' ? options.offset : 0;
        if (startingIndex < 0) startingIndex = 0;
        if (startingIndex > this.header.recordCount) startingIndex = this.header.recordCount;
        let finishedIndex =  typeof options.limit === 'number' ? startingIndex + options.limit : this.header.recordCount;
        if (finishedIndex < startingIndex) finishedIndex = startingIndex;
        if (finishedIndex > this.header.recordCount) finishedIndex = this.header.recordCount;

        // Calculate column name whitelist
        let columnWhitelist = this.columns.map(() => true);
        if (options.columnNames) {
            for (let i = 0; i < this.columns.length; ++i) {
                if (!options.columnNames.includes(this.columns[i].name)) {
                    columnWhitelist[i] = false;
                }
            }
        }

        let records = [] as Record[];
        let iteratedCount = 0;
        while (startingIndex + iteratedCount < finishedIndex) {
            let start  = this.header.dataOffset + this.header.recordLength * (startingIndex + iteratedCount);
            let end    = start + this.header.recordLength;
            let length = end - start;
            let tempBuffer = new Buffer(length);

            let {buffer} = await fsRead(this.fd, tempBuffer, 0, length, start);

            // TODO: desired semantics for deleted records????
            // read next record, unless it is marked as deleted (first byte = 0x05)
            if (buffer.readInt8(0) !== 5) {
                records.push(this.parseRecord(buffer, columnWhitelist));
            }

            ++iteratedCount;
        }
        return records;
    }

    async fetchRecord(recordNumber: number) {
        if (recordNumber > this.header.recordCount) {
            throw new Error(`record number ${recordNumber} is greater than the record count (${this.header.recordCount})`);
        }

        let start  = this.header.dataOffset + this.header.recordLength * recordNumber;
        let end    = start + this.header.recordLength;
        let length = end - start;
        let tempBuffer = new Buffer(length);

        let {buffer} = await fsRead(this.fd, tempBuffer, 0, length, start);
        let record = this.parseRecord(buffer, this.columns.map(() => true));
        return record;
    }

    async close() {
        if (this.fd !== -1) {
            let closed = fsClose(this.fd);
            this.fd = -1;
            await closed;
        }
    };

    private constructor(
        private encoding: string,
        private fd: number,
        private header: Header,
        private columns: Column[]
    ) {}

    // Determine record count, column count, record length, and data offset
    private static async parseHeader(fd: number) {
        let {buffer} = await fsRead(fd, new Buffer(HEADER_LENGTH), 0, HEADER_LENGTH, 0);
        let header = {} as Header;

        header.recordCount  = buffer.readUInt32LE(24);
        header.dataOffset   = buffer.readUInt32LE(32);
        header.recordLength = buffer.readUInt32LE(36);
        header.columnCount  = (header.dataOffset - 400) / 200;
        return header;
    }

    // Retrieves column information from the database
    private static async parseColumns(fd: number, encoding: string, header: Header) {
        // column information is located after the header
        // 200 bytes of information for each column
        let columnsLength = COLUMN_LENGTH * header.columnCount;
        let tempBuffer = new Buffer(HEADER_LENGTH + columnsLength);
        let {buffer} = await fsRead(fd, tempBuffer, 0, columnsLength, HEADER_LENGTH);

        let columns = [] as Column[];

        // NB: skip the first 5 bytes, don't know what they are for and they don't contain the data.
        let offset = 5;

        for (let i = 0; i < header.columnCount; ++i) {
            let column = buffer.slice(COLUMN_LENGTH * i);

            // column names are the first 128 bytes and column info takes up the last 72 bytes.
            // byte 130 contains a 16-bit column type
            // byte 136 contains a 16-bit length field
            let name = iconv.decode(column.slice(0, 128), encoding).replace(/\0/g, '').trim();
            let type = column.readUInt16LE(129);
            let length = column.readUInt16LE(135);
            columns.push({name: name, type: type, offset, length: length});
            offset += length;
        }
        return columns;
    }

    private parseRecord(buffer: Buffer, columnWhitelist: boolean[]) {
        let record = {} as Record;
        for (let i = 0; i < this.header.columnCount; ++i) {
            if (!columnWhitelist[i]) continue;
            let column = this.columns[i];
            record[column.name] = this.parseField(buffer, column.type, column.offset, column.length);
        }
        return record;
    }

    // Reference:
    // http://devzone.advantagedatabase.com/dz/webhelp/advantage8.1/server1/adt_field_types_and_specifications.htm
    private parseField(buffer: Buffer, type: number, start: number, length: number) {
        switch(type) {
            case ColumnType.CHARACTER:
            case ColumnType.CICHARACTER:
                return iconv.decode(buffer.slice(start, start + length), this.encoding).replace(/\0/g, '').trim();

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
                let b = iconv.decode(buffer.slice(start, start + length), this.encoding);
                return (b === 'T' || b === 't' || b === '1' || b === 'Y' || b === 'y' || b === ' ')

            case ColumnType.DATE:
                let julian = buffer.readInt32LE(start);
                return julian === 0 ? null : new Date((julian - JULIAN_1970) * MS_PER_DAY);

            case ColumnType.TIMESTAMP:
                let julian2 = buffer.readInt32LE(start);
                let ms = buffer.readInt32LE(start + 4);
                return julian2 === 0 && ms === -1 ? null : new Date((julian2 - JULIAN_1970) * MS_PER_DAY + ms);

            // not implemented
            case ColumnType.TIME:
                return buffer;

            default:
                return null;
        }
    }
}
