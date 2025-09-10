function sync(doc, oldDoc, meta) {
  // Validate that the document has id
  if (!doc.id) {
    throw({forbidden: "Document must contain an encounterId field"});
  }

  // Create dynamic channel based on id
  var encounterChannel = "tx_Encounter_" + doc.id;

  // Assign document to this channel
  channel(encounterChannel);

  
}