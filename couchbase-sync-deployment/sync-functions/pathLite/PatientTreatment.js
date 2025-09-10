function sync(doc, oldDoc, meta) {
  // Validate that the document has hospitalId
  if (!doc.hospitalId) {
    throw({forbidden: "Document must contain a hospitalId field"});
  }

  // Create dynamic channel based on hospitalId
  var treatmentChannel = "tx_PatientTreatment_" + doc.hospitalId;

  // Assign document to this channel
  channel(treatmentChannel);

}