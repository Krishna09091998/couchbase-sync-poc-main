function sync(doc, oldDoc, meta) {
  if (!doc.encounterId) {
        throw({ forbidden: "Document must have encounterId" });
    }

    requireUser(doc.owner); // Only allow the owner to write

    var channelId = "tx_PatientEduction_" + doc.encounterId;

    channel(channelId);                 // Assigning document to the channel
    access(doc.owner, channelId);      //granting the access to the channel
}