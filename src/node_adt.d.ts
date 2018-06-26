declare module "node_adt" {


    export = Adt;


    class Adt {
        open(path: string, encoding: string, callback: (err: any, table: Table) => void): void;
    }


    class Table {
        header: TableHeader;
        columns: TableColumn[];
        eachRecord(options: {limit?: number, offset?: number}, iterator: (err: any, record: {[name: string]: any}) => void, callback: (err: any) => void): void;
        eachRecord(iterator: (err: any, record: {[name: string]: any}) => void, callback: (err: any) => void): void;
        close(): void;
    }


    interface TableHeader {
        recordCount: number;
        columnCount: number;
        recordLength: number;
        dataOffset: number;
    }


    interface TableColumn {
        name: string;
        type: number;
        length: number;
    }
}
