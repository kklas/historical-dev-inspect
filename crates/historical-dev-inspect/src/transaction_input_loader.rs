use sui_types::{
    base_types::{FullObjectID, ObjectRef},
    committee::EpochId,
    error::{SuiError, SuiResult, UserInputError},
    storage::FullObjectKey,
    transaction::{
        InputObjectKind, InputObjects, ObjectReadResult, ObjectReadResultKind,
        ReceivingObjectReadResult, ReceivingObjectReadResultKind, ReceivingObjects,
    },
};

use crate::store::HistoricalView;

pub fn read_objects_for_signing(
    store: &HistoricalView,
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

    let objects = store.multi_get_objects_with_more_accurate_error_return(&object_refs)?;
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

pub fn read_receiving_objects_for_signing(
    store: &HistoricalView,
    receiving_objects: &[ObjectRef],
    epoch_id: EpochId,
) -> SuiResult<ReceivingObjects> {
    let mut receiving_results = Vec::with_capacity(receiving_objects.len());
    for objref in receiving_objects {
        // Note: the digest is checked later in check_transaction_input
        let (object_id, version, _) = objref;

        // TODO: Add support for receiving ConsensusV2 objects. For now this assumes fastpath.
        if store.have_received_object_at_version(
            FullObjectKey::new(FullObjectID::new(*object_id, None), *version),
            epoch_id,
        ) {
            receiving_results.push(ReceivingObjectReadResult::new(
                *objref,
                ReceivingObjectReadResultKind::PreviouslyReceivedObject,
            ));
            continue;
        }

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
