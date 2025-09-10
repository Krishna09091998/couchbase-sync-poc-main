function sync(doc, oldDoc, meta) {
  if (!doc.hospitalId) {
        throw({ forbidden: "Document must have valid hospitalId" });
    }
    var channelId = "hc_HospitalConfiguration_" + doc.hospitalId;
    channel(channelId); // Assigning document to the channel
}