function (doc, oldDoc, meta) {
	// Require that only users with role_masterdata can write
       requireRole("role_masterdata");
  // Validate that the document must contain releaseType and appType
    if (!doc.releaseType || !doc.appType) {
        throw({forbidden: "Document must contain a releaseType and apptype"});
    }
// Define dynamic channel names based on releaseType
  var releaseType = doc.releaseType.toLowerCase();
  var appType = doc.appType === 'PATH-LITE' ? 'pathlite': doc.appType.toLowerCase();
  var channelName = "master_" + appType + "_" + releaseType + "_ReportSegment";
  channel(channelName)
}