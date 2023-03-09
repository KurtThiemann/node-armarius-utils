import NodeFileReader from "./src/NodeFileReader.js";
import {ReadArchive} from "armarius";
import fs from "node:fs";
import pathModule from "node:path";

/**
 * @param {import("armarius").DataReader} reader
 * @param {import("armarius").ReadArchiveOptions|import("armarius").ReadArchiveOptionsObject} options
 * @return {Promise<ReadArchive>}
 */
export async function open(reader, options) {
    const archive = new ReadArchive(reader, options);
    await archive.init();
    return archive;
}

/**
 * @param path
 * @return {Promise<NodeFileReader>}
 */
export async function openFile(path) {
    return await NodeFileReader.open(path);
}

export async function extract(path, options, destination, basePath = '') {
    if(basePath.length && !basePath.endsWith('/')) {
        basePath += '/';
    }
    const reader = await openFile(path);
    const archive = await open(reader, options);
    let entries = await archive.getEntryIterator();
    let entry;
    while (entry = await entries.next()) {
        const name = entry.getFileNameString();
        if(!name.startsWith(basePath)) {
            continue;
        }

        let fullPath = `${destination}/${name.substring(basePath.length)}`;
        if(entry.isDirectory()) {
            await fs.promises.mkdir(fullPath, {recursive: true});
            continue;
        }

        await fs.promises.mkdir(pathModule.dirname(fullPath), {recursive: true});
        let output = fs.createWriteStream(fullPath);
        let dataReader = await entry.getDataReader();
        let chunk;
        while ((chunk = await dataReader.read(1024 * 256)) !== null) {
            await writeToStream(output, chunk);
        }
        await endStream(output);
    }
    await reader.close();
}

function writeToStream(stream, data) {
    return new Promise((resolve, reject) => {
        stream.write(data, (err) => {
            if(err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function endStream(stream) {
    return new Promise((resolve, reject) => {
        stream.end((err) => {
            if(err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}


