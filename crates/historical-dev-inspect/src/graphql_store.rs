use std::sync::Arc;

use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber},
    committee::EpochId,
    error::{SuiError, SuiErrorKind, SuiResult, UserInputError},
    object::{Object, Owner},
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, PackageObject, ParentSync},
};

use crate::graphql::{GraphqlClient, GraphqlError};

pub struct GraphqlView {
    client: Arc<GraphqlClient>,
    checkpoint: u64,
}

impl GraphqlView {
    pub fn new(client: Arc<GraphqlClient>, checkpoint: u64) -> Self {
        Self { client, checkpoint }
    }
}

fn to_sui_error(e: GraphqlError) -> SuiError {
    SuiError::from(SuiErrorKind::BadObjectType {
        error: e.to_string(),
    })
}

impl ObjectStore for GraphqlView {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.client
            .fetch_object_at_checkpoint(*object_id, self.checkpoint)
            .expect("GraphQL fetch failed")
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> Option<Object> {
        self.client
            .fetch_object_at_version(*object_id, version.value())
            .expect("GraphQL fetch failed")
    }
}

impl ChildObjectResolver for GraphqlView {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let child_object = self
            .client
            .fetch_object_at_root_version(*child, child_version_upper_bound.value())
            .map_err(to_sui_error)?;

        let Some(child_object) = child_object else {
            return Ok(None);
        };

        if child_object.owner != Owner::ObjectOwner((*parent).into()) {
            return Err(SuiError::from(SuiErrorKind::InvalidChildObjectAccess {
                object: *child,
                given_parent: *parent,
                actual_owner: child_object.owner.clone(),
            }));
        }

        Ok(Some(child_object))
    }

    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        _epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        let Some(recv_object) =
            self.get_object_by_key(receiving_object_id, receive_object_at_version)
        else {
            return Ok(None);
        };

        // For dev-inspect, have_received_object_at_version is always false.
        if recv_object.owner != Owner::AddressOwner((*owner).into()) {
            return Ok(None);
        }

        Ok(Some(recv_object))
    }
}

impl BackingPackageStore for GraphqlView {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        let Some(obj) = self.get_object(package_id) else {
            return Ok(None);
        };

        if obj.is_package() {
            Ok(Some(PackageObject::new(obj)))
        } else {
            Err(UserInputError::MoveObjectAsPackage {
                object_id: *package_id,
            }
            .into())
        }
    }
}

impl ParentSync for GraphqlView {
    fn get_latest_parent_entry_ref_deprecated(&self, object_id: ObjectID) -> Option<ObjectRef> {
        self.get_object(&object_id)
            .map(|obj| obj.compute_object_reference())
    }
}
