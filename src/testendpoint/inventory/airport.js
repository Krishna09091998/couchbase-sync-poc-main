module.exports = `
function (doc, oldDoc, meta) {
    // Example: allow reads for everyone
    if (doc.type === "public") {
        return true;
    }
    return false;
}
`;