// tslint:disable-next-line: interface-name
export declare interface RequestLogger {
    new(logger: any, logLevel: string, dumpThreshold: string, endLevel: string, uids?: string[] | string): any;
    getUids(): string[];
    getSerializedUids(): string;
    trace(msg: string, data?: any): void;
    debug(msg: string, data?: any): void;
    info(msg: string, data?: any): void;
    warn(msg: string, data?: any): void;
    error(msg: string, data?: any): void;
}
