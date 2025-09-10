function sync(doc, oldDoc, meta) {
  // Validate that the document has hospitalId
  if (!doc.hospitalId) {
    throw({forbidden: "Document must contain a hospitalId field"});
  }

  // Create dynamic channel based on hospitalId
  var recallChannel = "tx_PatientRecallState_" + doc.hospitalId;

  // Assign document to this channel
  channel(recallChannel);  
}