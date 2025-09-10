function sync(doc, oldDoc, meta) {
  // Validate that the document has hospitalId
  if (!doc.hospitalId) {
    throw({forbidden: "Document must contain a hospitalId field"});
  }

  // Create dynamic channel based on hospitalId
  var reportsChannel = "tx_Reports_" + doc.hospitalId;

  // Assign document to this channel
  channel(reportsChannel);

}