function sync(doc, oldDoc, meta) {
  if (!doc.encounterId) {
        throw({ forbidden: "Document must have encounterId" });
    }

   // requireUser(doc.owner); // Only allow the owner to write

    var channelId = "tx_Reports_" + doc.hospitalId;

    channel(channelId);                 // Assigning document to the channel
   // access(doc.?, channelId);      //granting the access to the channel
}