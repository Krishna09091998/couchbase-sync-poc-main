function sync(doc, oldDoc, meta) {
  if (!doc.hospitalId) {
        throw({ forbidden: "Document must have valid hospitalId" });
    }
    var channelId = "hc_DeviceConfiguration_" + doc.hospitalId;
    channel(channelId); // Assigning document to the channel
}