function sync(doc, oldDoc, meta) {
  requireRole("role_masterdata");
  if (!doc.releaseType || !doc.appType) {
    throw({forbidden: "Document must contain a releaseType and apptype"});
  }
  var releaseType = doc.releaseType.toLowerCase();
  var appType = doc.appType === "PATH-LITE" ? "pathlite" : doc.appType.toLowerCase();
  var channelName = "master_" + appType + "_" + releaseType + "_EmailNotification";
  channel(channelName);
}