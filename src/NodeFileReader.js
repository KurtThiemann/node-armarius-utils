import {ArmariusError, DataReader} from "armarius";
import fs from "node:fs";

export default class NodeFileReader extends DataReader {
    /**  @type {FileHandle} */ file;
    /** @type {?Uint8Array} */ buffer;
    /** @type {DataView} */ bufferView;
    /** @type {Number} */ bufferStartOffset;
    /** @type {boolean} */ blocked = false;

    /**
     * @param {string} path
     * @return {Promise<NodeFileReader>}
     */
    static async open(path) {
        let file = await fs.promises.open(path, 'r');
        let stat = await file.stat();
        return new this(file, 0, stat.size);
    }

    /**
     * @param {FileHandle} file
     * @param {number} byteOffset
     * @param {number} byteLength
     */
    constructor(file, byteOffset, byteLength) {
        super();
        this.file = file;
        this.byteLength = byteLength;
        this.byteOffset = byteOffset;
        if (this.byteLength < 0) {
            throw new ArmariusError('Invalid file range');
        }
    }

    /**
     * @inheritDoc
     */
    async readAt(offset, length, longLived = true) {
        if (this.buffer && offset >= this.bufferStartOffset && length <= this.buffer.byteLength - (offset - this.bufferStartOffset)) {
            return this.readFromBuffer(offset, length, longLived);
        }

        if (length < this.bufferSize) {
            this.setBuffer(offset, await this.readRaw(offset, Math.max(length, Math.min(this.bufferSize, this.byteLength - offset))));
            return this.readFromBuffer(offset, length, longLived);
        }

        return await this.readRaw(offset, length);
    }

    /**
     * @param {number} offset
     * @param {number} length
     * @protected
     * @returns {Promise<Uint8Array>}
     */
    async readRaw(offset, length) {
        if(this.blocked) {
            throw new Error('Multiple simultaneous reads are not supported');
        }
        this.blocked = true;

        if (offset < 0) {
            throw new ArmariusError(`Cannot read at negative offsets (got ${offset})`);
        }
        if (offset + length > this.byteLength) {
            throw new ArmariusError(`Cannot read beyond end of data (trying to read ${length} bytes at ${offset}, data length is ${this.byteLength})`);
        }
        const data = Buffer.alloc(length);
        await this.file.read({position: this.byteOffset + offset, length, buffer: data});
        return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }

    /**
     * @param {number} offset
     * @param {number} length
     * @param {boolean} longLived
     * @protected
     * @returns {Uint8Array}
     */
    readFromBuffer(offset, length, longLived = false) {
        let bufferOffset = offset - this.bufferStartOffset;
        if (bufferOffset < 0 || bufferOffset + length > this.buffer.byteLength) {
            throw new ArmariusError(`Cannot read ${length} bytes of buffer at ${bufferOffset}`);
        }

        if (longLived && this.buffer.byteLength - length > 512) {
            return this.buffer.slice(bufferOffset, bufferOffset + length);
        }

        return new Uint8Array(this.buffer.buffer, this.buffer.byteOffset + bufferOffset, length);
    }

    /**
     * @param {number} bufferOffset
     * @param {Uint8Array} data
     * @protected
     */
    setBuffer(bufferOffset, data) {
        this.buffer = data;
        this.bufferView = new DataView(data.buffer, data.byteOffset, data.byteLength);
        this.bufferStartOffset = bufferOffset;
    }

    /**
     * @inheritDoc
     */
    async getUint8At(offset) {
        if (this.buffer && offset > this.bufferStartOffset && offset - this.bufferStartOffset + 1 < this.buffer.byteLength) {
            return this.bufferView.getUint8(offset - this.bufferStartOffset);
        }
        return super.getUint8At(offset);
    }

    /**
     * @inheritDoc
     */
    async getUint16At(offset, littleEndian = true) {
        if (this.buffer && offset > this.bufferStartOffset && offset - this.bufferStartOffset + 2 < this.buffer.byteLength) {
            return this.bufferView.getUint16(offset - this.bufferStartOffset, littleEndian);
        }
        return super.getUint16At(offset, littleEndian);
    }

    /**
     * @inheritDoc
     */
    async getUint32At(offset, littleEndian = true) {
        if (this.buffer && offset > this.bufferStartOffset && offset - this.bufferStartOffset + 4 < this.buffer.byteLength) {
            return this.bufferView.getUint32(offset - this.bufferStartOffset, littleEndian);
        }
        return super.getUint32At(offset, littleEndian);
    }

    /**
     * @inheritDoc
     */
    async getBigUint64At(offset, littleEndian = true) {
        if (this.buffer && offset > this.bufferStartOffset && offset - this.bufferStartOffset + 8 < this.buffer.byteLength) {
            return this.bufferView.getBigUint64(offset - this.bufferStartOffset, littleEndian);
        }
        return super.getBigUint64At(offset, littleEndian);
    }

    /**
     * @inheritDoc
     */
    async clone(cloneOffset = 0, cloneLength = null) {
        if (cloneLength === null) {
            cloneLength = this.byteLength - cloneOffset;
        }
        return new this.constructor(this.file, this.byteOffset + cloneOffset, cloneLength)
            .setMaxBufferSize(this.bufferSize);
    }

    /**
     * @return {Promise<this>}
     */
    async close() {
        await this.file.close();
        return this;
    }
}
