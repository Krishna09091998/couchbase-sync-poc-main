function sync(doc, oldDoc, meta) {
    if (!doc.hospitalId) {
        throw({ forbidden: "Document must have hospitalId" });
    }
   // var hospitalRole = "role_tx_PatientRecallState_" + doc.hospitalId ;
   // requireRole(hospitalRole)

    var channelId = "tx_PatientRecallState_" + doc.hospitalId ;

    channel(channelId);                 // Assigning document to the channel
    //access(doc.?, channelId);   // Grant access to that user based on ..?
}