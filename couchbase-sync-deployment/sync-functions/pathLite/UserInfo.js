function sync(doc, oldDoc, meta) {
  // Validate that the document has a userLogin
  if (!doc.userLogin) {
    throw({forbidden: "Document must contain a userLogin field"});
  }

  // Create a dynamic channel based on userLogin
  var userChannel = "userInfo_" + doc.userLogin;

  // Assign document to the channel
  channel(userChannel);

}