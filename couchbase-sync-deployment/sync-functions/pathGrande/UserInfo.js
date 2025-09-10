function sync(doc, oldDoc, meta) {
    if (!doc.userLogin) {
        throw({ forbidden: "Document must have userLogin" });
    }

    requireUser(doc.userLogin); // Only allow the owner to write

    var channelId = "userInfo_" + doc.userLogin;

    channel(channelId);                 // Assign document to user+hospital-specific channel
    access(doc.userLogin, channelId);     // Grant access to that user
   
    var roleNamesList = [];
    //if permissions arrays exists and is valid :
    // if (doc.permissions && Array.isArray(doc.permissions)) {
          if (doc.permissions && doc.permissions.length>0) {
          for(var i=0; i<doc.permissions.length; i++){
            var permission = doc.permissions[i];
            //ensure hospital object and acctNumber are present: 
            if(permission.hospital && permission.hospital.hospitalAcctNumber){
              var acctNumber=permission.hospital.hospitalAcctNumber;

              roleNamesList.push("role:role_tx_PatientTreatment_" + acctNumber);

            }

          }
          //Assigning the role:
          // role(doc.teammateId, roleNamesList);
          role(doc.userLogin, roleNamesList);
    }
}