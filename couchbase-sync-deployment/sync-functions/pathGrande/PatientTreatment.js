function sync1(doc, oldDoc, meta) {
    if (!doc.encounterId) {
        throw({ forbidden: "Document must have encounterId" });
    }
    var hospitalRole = "role_tx_PatientTreatment_" + doc.hospitalId ;
    requireRole(hospitalRole)
    var channelId = "tx_PatientTreatment_" + doc.hospitalId ;

    channel(channelId);                 // Assign document to user+hospital-specific channel
    // access(doc.ownerId, channelId);     // Grant access to that user
}