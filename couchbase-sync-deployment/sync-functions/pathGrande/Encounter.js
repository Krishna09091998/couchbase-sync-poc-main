function sync(doc, oldDoc, meta) {
    var treatmentChannel = "tx_Encounter_" + doc.encounterId;
    var atlasChannel = "tx_Encounter_" + doc.hospitalId;

    // Assign dynamic channel based on treatment ID
    channel(treatmentChannel);
    channel(atlasChannel);

    // Grant access to current owner
    if (doc.ownerId) {
        access(doc.ownerId, treatmentChannel);
    }

    // Remove access from previous owner (if changed)
    if (oldDoc && oldDoc.ownerId && oldDoc.ownerId !== doc.ownerId) {
        // Do NOT call access(oldDoc.owner, []); — that revokes ALL channels
        // Instead, rely on Sync Gateway to revoke channel access automatically
        // by NOT granting it again
    }

}