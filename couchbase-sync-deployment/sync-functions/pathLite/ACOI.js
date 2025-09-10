function sync(doc, oldDoc, meta) {
  // Validate that the document has encounterId
  if (!doc.encounterId) {
    throw({forbidden: "Document must contain an encounterId field"});
  }

  // Create dynamic channel based on encounterId
  var acoiChannel = "tx_ACOI_" + doc.encounterId;

  // Assign document to this channel
  channel(acoiChannel);
}
