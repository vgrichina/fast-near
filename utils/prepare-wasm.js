// NOTE: Looks like nearcore preprocesses WASM it's own way, so this code tries to match
// https://github.com/near/nearcore/blob/85563483db8f7655cbb45e856ba3fb99bbf463e3/runtime/near-vm-runner/src/prepare.rs#L143
// Binary format is manipulated directly, as otherwise is super slow (seconds) using `@webassemblyjs/wasm-edit`
// https://coinexsmartchain.medium.com/wasm-introduction-part-1-binary-format-57895d851580

const debug = require('debug')('prepare-wasm');

function prepareWASM(input) {
    const parts = [];

    const magic = input.slice(0, 4);
    if (magic.toString('utf8') !== '\0asm') {
        throw new Error('Invalid magic number');
    }

    const version = input.readUInt32LE(4);
    if (version != 1) {
        throw new Error('Invalid version: ' + version);
    }

    let offset = 8;
    parts.push(input.slice(0, offset));

    function decodeLEB128() {
        let result = 0;
        let shift = 0;
        let byte;
        do {
            byte = input[offset++];
            result |= (byte & 0x7f) << shift;
            shift += 7;
        } while (byte & 0x80);
        return result;
    }

    function decodeLimits() {
        const flags = input[offset++];
        const hasMax = flags & 0x1;
        const initial = decodeLEB128();
        const max = hasMax ? decodeLEB128() : null;
        return { initial, max };
    }

    function decodeString() {
        const length = decodeLEB128();
        const result = input.slice(offset, offset + length);
        offset += length;
        return result.toString('utf8');
    }

    function encodeLEB128(value) {
        const result = [];
        do {
            let byte = value & 0x7f;
            value >>= 7;
            if (value !== 0) {
                byte |= 0x80;
            }
            result.push(byte);
        } while (value !== 0);
        return Buffer.from(result);
    }

    function encodeString(value) {
        const result = Buffer.from(value, 'utf8');
        return Buffer.concat([encodeLEB128(result.length), result]);
    }

    do {
        const sectionStart = offset;
        const sectionId = input.readUInt8(offset);
        offset++;
        const sectionSize = decodeLEB128();
        const sectionEnd = offset + sectionSize;
        debug('section', sectionId, sectionStart, sectionSize);
        
        if (sectionId == 5) {
            // Memory section
            // Make sure it's empty and only imported memory is used
            parts.push(Buffer.from([5, 1, 0]));
        } else if (sectionId == 2) {
            // Import section
            const sectionParts = [];
            const numImports = decodeLEB128();
            debug('numImports', numImports);
            for (let i = 0; i < numImports; i++) {
                const importStart = offset;
                const module = decodeString();
                const field = decodeString();
                const kind = input.readUInt8(offset);
                debug('offset', offset.toString(16), 'kind', kind, 'module', module, 'field', field);
                offset++;
                
                let skipImport = false;
                switch (kind) {
                    case 0:
                        // Function import
                        decodeLEB128(); // index
                        break;
                    case 1:
                        // Table import
                        offset++; // type
                        decodeLimits();
                        break;
                    case 2:
                        // Memory import
                        decodeLimits();
                        // NOTE: existing memory import is removed (so no need to add it to sectionParts)
                        skipImport = true;
                        break;
                    case 3:
                        // Global import
                        offset++; // type
                        offset++; // mutability
                        break;
                    default:
                        throw new Error('Invalid import kind: ' + kind);
                }

                if (!skipImport) {
                    sectionParts.push(input.slice(importStart, offset));
                }
            }

            const importMemory = Buffer.concat([
                encodeString('env'),
                encodeString('memory'),
                Buffer.from([2]), // Memory import
                // TODO: Check what values to use
                Buffer.from([0]),
                encodeLEB128(1),
            ]);

            sectionParts.push(importMemory);

            const sectionData = Buffer.concat([
                encodeLEB128(sectionParts.length),
                ...sectionParts,
            ]);

            parts.push(Buffer.concat([
                Buffer.from([2]), // Import section
                encodeLEB128(sectionData.length),
                sectionData
            ]));
        } else if (sectionId == 7) {
            // Export section
            const sectionParts = [];
            const numExports = decodeLEB128();
            debug('numExports', numExports);
            for (let i = 0; i < numExports; i++) {
                const exportStart = offset;
                const name = decodeString();
                const kind = input.readUInt8(offset);
                offset++;
                const index = decodeLEB128();
                debug('kind', kind, 'name', name, 'index', index);
                if (kind !== 2) {
                    // Pass through all exports except memory
                    sectionParts.push(input.slice(exportStart, offset));
                }
            }

            const sectionData = Buffer.concat([
                encodeLEB128(sectionParts.length),
                ...sectionParts,
            ]);

            parts.push(Buffer.concat([
                Buffer.from([7]), // Export section
                encodeLEB128(sectionData.length),
                sectionData
            ]));
        } else {
            parts.push(input.slice(sectionStart, sectionEnd));
        }

        offset = sectionEnd;
    } while (offset < input.length);

    return Buffer.concat(parts);
}

module.exports = { prepareWASM };