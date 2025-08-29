function syncFunction(doc, oldDoc) {
  if (!doc.type) {
    throw({forbidden: "Document must have a type"});
  }

  if (doc.type === "airport") {
    channel("airports");
    access("airport_admin", doc.owner || "default_owner");
  } else if (doc.type === "route") {
    channel("routes");
  } else {
    channel(doc.type);
  }

  if (oldDoc && doc._deleted && doc.type === "airport") {
    throw({forbidden: "Cannot delete airport documents"});
  }
}
