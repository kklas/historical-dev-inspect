use sui_types::{
    base_types::ObjectRef,
    committee::EpochId,
    error::{SuiError, SuiResult, UserInputError},
    object::Object,
    storage::{BackingPackageStore, ObjectStore},
    transaction::{
        InputObjectKind, InputObjects, ObjectReadResult, ObjectReadResultKind,
        ReceivingObjectReadResult, ReceivingObjects,
    },
};

pub fn read_objects_for_signing(
    store: &(impl ObjectStore + BackingPackageStore),
    input_object_kinds: &[InputObjectKind],
    receiving_objects: &[ObjectRef],
    epoch_id: EpochId,
) -> SuiResult<(InputObjects, ReceivingObjects)> {
    // Length of input_object_kinds have been checked via validity_check() for ProgrammableTransaction.
    let mut input_results = vec![None; input_object_kinds.len()];
    let mut object_refs = Vec::with_capacity(input_object_kinds.len());
    let mut fetch_indices = Vec::with_capacity(input_object_kinds.len());

    for (i, kind) in input_object_kinds.iter().enumerate() {
        match kind {
            // Packages are loaded one at a time via the cache
            InputObjectKind::MovePackage(id) => {
                let Some(package) = store.get_package_object(id)?.map(|o| o.into()) else {
                    return Err(SuiError::from(kind.object_not_found_error()));
                };
                input_results[i] = Some(ObjectReadResult {
                    input_object_kind: *kind,
                    object: ObjectReadResultKind::Object(package),
                });
            }
            InputObjectKind::SharedMoveObject {
                id,
                /* initial_shared_version, */
                ..
            } => match store.get_object(id) {
                Some(object) => {
                    input_results[i] = Some(ObjectReadResult::new(*kind, object.into()))
                }
                None => {
                    return Err(SuiError::from(kind.object_not_found_error()));
                    // TODO: handle deleted shared objects?
                    /*
                    if let Some((version, digest)) = store.get_last_shared_object_deletion_info(
                        FullObjectID::new(*id, Some(*initial_shared_version)),
                        epoch_id,
                    ) {
                        input_results[i] = Some(ObjectReadResult {
                            input_object_kind: *kind,
                            object: ObjectReadResultKind::DeletedSharedObject(version, digest),
                        });
                    } else {
                        return Err(SuiError::from(kind.object_not_found_error()));
                    }
                    */
                }
            },
            InputObjectKind::ImmOrOwnedMoveObject(objref) => {
                object_refs.push(*objref);
                fetch_indices.push(i);
            }
        }
    }

    let objects = multi_get_objects_with_more_accurate_error_return(store, &object_refs)?;
    assert_eq!(objects.len(), object_refs.len());
    for (index, object) in fetch_indices.into_iter().zip(objects.into_iter()) {
        input_results[index] = Some(ObjectReadResult {
            input_object_kind: input_object_kinds[index],
            object: ObjectReadResultKind::Object(object),
        });
    }

    let receiving_results = read_receiving_objects_for_signing(store, receiving_objects, epoch_id)?;

    Ok((
        input_results
            .into_iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>()
            .into(),
        receiving_results,
    ))
}

/// Load a list of objects from the store by object reference.
/// If they exist in the store, they are returned directly.
/// If any object missing, we try to figure out the best error to return.
/// If the object we are asking is currently locked at a future version, we know this
/// transaction is out-of-date and we return a ObjectVersionUnavailableForConsumption,
/// which indicates this is not retriable.
/// Otherwise, we return a ObjectNotFound error, which indicates this is retriable.
pub fn multi_get_objects_with_more_accurate_error_return(
    store: &impl ObjectStore,
    object_refs: &[ObjectRef],
) -> Result<Vec<Object>, SuiError> {
    let objects: Vec<Option<Object>> = object_refs
        .iter()
        .map(|r| store.get_object_by_key(&r.0, r.1))
        .collect();
    let mut result = Vec::new();
    for (object_opt, object_ref) in objects.into_iter().zip(object_refs) {
        match object_opt {
            None => {
                let live_objref = match store.get_object(&object_ref.0) {
                    Some(obj) => obj.compute_object_reference(),
                    None => {
                        return Err(UserInputError::ObjectNotFound {
                            object_id: object_ref.0,
                            version: None,
                        }
                        .into());
                    }
                };
                let error: UserInputError = if live_objref.1 >= object_ref.1 {
                    UserInputError::ObjectVersionUnavailableForConsumption {
                        provided_obj_ref: *object_ref,
                        current_version: live_objref.1,
                    }
                } else {
                    UserInputError::ObjectNotFound {
                        object_id: object_ref.0,
                        version: Some(object_ref.1),
                    }
                };
                return Err(error.into());
            }
            Some(object) => {
                result.push(object);
            }
        }
    }
    assert_eq!(result.len(), object_refs.len());
    Ok(result)
}

pub fn read_receiving_objects_for_signing(
    store: &impl ObjectStore,
    receiving_objects: &[ObjectRef],
    _epoch_id: EpochId,
) -> SuiResult<ReceivingObjects> {
    let mut receiving_results = Vec::with_capacity(receiving_objects.len());
    for objref in receiving_objects {
        // Note: the digest is checked later in check_transaction_input
        let (object_id, version, _) = objref;

        // For dev-inspect, we always return false for have_received_object_at_version.
        // This is safe because it only affects double-receive detection which doesn't
        // matter in simulation.

        let Some(object) = store.get_object(object_id) else {
            return Err(UserInputError::ObjectNotFound {
                object_id: *object_id,
                version: Some(*version),
            }
            .into());
        };

        receiving_results.push(ReceivingObjectReadResult::new(*objref, object.into()));
    }
    Ok(receiving_results.into())
}
