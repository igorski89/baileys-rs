use super::{Error, Event, Socket};
use crate::{
    binary::{BinaryNode, NodeContent},
    communities::{self, LinkedGroups, Metadata},
    groups::{
        self, JoinApprovalMode, MemberAddMode, ParticipantAction, ParticipantResult, QueryType,
        RequestAction, Setting,
    },
};
use indexmap::IndexMap;

impl Socket {
    pub async fn community_metadata(&self, jid: &str) -> Result<Metadata, Error> {
        let result = self
            .group_query(
                jid,
                QueryType::Get,
                vec![BinaryNode::new("query").attr("request", "interactive")],
            )
            .await?;
        Ok(communities::extract_metadata(&result)?)
    }
    pub async fn community_fetch_all_participating(
        &self,
    ) -> Result<IndexMap<String, Metadata>, Error> {
        let result = self
            .query(
                BinaryNode::new("iq")
                    .attr("to", "@g.us")
                    .attr("xmlns", "w:g2")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("participating").with_content(NodeContent::Children(vec![
                            BinaryNode::new("participants"),
                            BinaryNode::new("description"),
                        ])),
                    ])),
                None,
            )
            .await?;
        let metadata = communities::extract_participating(&result)?;
        let _ = self.inner.events.send(Event::GroupsUpdated(
            metadata.values().cloned().map(Into::into).collect(),
        ));
        Ok(metadata)
    }
    // A created group is reported through its metadata; failures yield no value.
    async fn created_group(&self, result: &BinaryNode) -> Option<groups::Metadata> {
        tracing::info!(tag = %result.tag, id = ?result.attrs.get("id"), "parseGroupResult");
        let group = result.child("group")?;
        tracing::info!(id = ?group.attrs.get("id"), "groupNode");
        let id = group.attrs.get("id")?;
        self.group_metadata(&format!("{id}@g.us")).await.ok()
    }
    /// Create a community with a general chat and approval-required membership.
    pub async fn community_create(
        &self,
        subject: &str,
        body: &str,
    ) -> Result<Option<groups::Metadata>, Error> {
        let description_id = crate::messages::generate_id()?[..12].to_owned();
        let result = self
            .group_query(
                "@g.us",
                QueryType::Set,
                vec![
                    BinaryNode::new("create")
                        .attr("subject", subject)
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("description")
                                .attr("id", description_id)
                                .with_content(NodeContent::Children(vec![
                                    BinaryNode::new("body")
                                        .with_content(NodeContent::Bytes(body.as_bytes().to_vec())),
                                ])),
                            BinaryNode::new("parent")
                                .attr("default_membership_approval_mode", "request_required"),
                            BinaryNode::new("allow_non_admin_sub_group_creation"),
                            BinaryNode::new("create_general_chat"),
                        ])),
                ],
            )
            .await?;
        Ok(self.created_group(&result).await)
    }
    pub async fn community_create_group(
        &self,
        subject: &str,
        participants: &[String],
        parent_community_jid: &str,
    ) -> Result<Option<groups::Metadata>, Error> {
        let mut content = groups::participant_nodes(participants);
        content.push(BinaryNode::new("linked_parent").attr("jid", parent_community_jid));
        let result = self
            .group_query(
                "@g.us",
                QueryType::Set,
                vec![
                    BinaryNode::new("create")
                        .attr("subject", subject)
                        .attr("key", crate::messages::generate_id_v2(None)?)
                        .with_content(NodeContent::Children(content)),
                ],
            )
            .await?;
        Ok(self.created_group(&result).await)
    }
    pub async fn community_leave(&self, id: &str) -> Result<(), Error> {
        self.group_query(
            "@g.us",
            QueryType::Set,
            vec![
                BinaryNode::new("leave").with_content(NodeContent::Children(vec![
                    BinaryNode::new("community").attr("id", id),
                ])),
            ],
        )
        .await?;
        Ok(())
    }
    pub async fn community_update_subject(&self, jid: &str, subject: &str) -> Result<(), Error> {
        self.group_update_subject(jid, subject).await
    }
    pub async fn community_link_group(
        &self,
        group_jid: &str,
        parent_community_jid: &str,
    ) -> Result<(), Error> {
        self.group_query(
            parent_community_jid,
            QueryType::Set,
            vec![
                BinaryNode::new("links").with_content(NodeContent::Children(vec![
                    BinaryNode::new("link")
                        .attr("link_type", "sub_group")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("group").attr("jid", group_jid),
                        ])),
                ])),
            ],
        )
        .await?;
        Ok(())
    }
    pub async fn community_unlink_group(
        &self,
        group_jid: &str,
        parent_community_jid: &str,
    ) -> Result<(), Error> {
        self.group_query(
            parent_community_jid,
            QueryType::Set,
            vec![
                BinaryNode::new("unlink")
                    .attr("unlink_type", "sub_group")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("group").attr("jid", group_jid),
                    ])),
            ],
        )
        .await?;
        Ok(())
    }
    /// List the sub-groups of a community, given the community or one of its sub-groups.
    pub async fn community_fetch_linked_groups(&self, jid: &str) -> Result<LinkedGroups, Error> {
        let metadata = self.group_metadata(jid).await?;
        let (community_jid, is_community) = match metadata.linked_parent {
            Some(parent) => (parent, false),
            None => (jid.to_owned(), true),
        };
        let result = self
            .group_query(
                &community_jid,
                QueryType::Get,
                vec![BinaryNode::new("sub_groups")],
            )
            .await?;
        Ok(LinkedGroups {
            community_jid,
            is_community,
            linked_groups: communities::extract_linked_groups(&result),
        })
    }
    pub async fn community_request_participants_list(
        &self,
        jid: &str,
    ) -> Result<Vec<IndexMap<String, String>>, Error> {
        self.group_request_participants_list(jid).await
    }
    pub async fn community_request_participants_update(
        &self,
        jid: &str,
        participants: &[String],
        action: RequestAction,
    ) -> Result<Vec<ParticipantResult>, Error> {
        self.group_request_participants_update(jid, participants, action)
            .await
    }
    /// Removing a member also removes them from the community's linked groups.
    pub async fn community_participants_update(
        &self,
        jid: &str,
        participants: &[String],
        action: ParticipantAction,
    ) -> Result<Vec<ParticipantResult>, Error> {
        let mut node = BinaryNode::new(action.tag());
        if matches!(action, ParticipantAction::Remove) {
            node = node.attr("linked_groups", "true");
        }
        let result = self
            .group_query(
                jid,
                QueryType::Set,
                vec![
                    node.with_content(NodeContent::Children(groups::participant_nodes(
                        participants,
                    ))),
                ],
            )
            .await?;
        Ok(groups::participant_results(
            result.child(action.tag()),
            true,
        ))
    }
    pub async fn community_update_description(
        &self,
        jid: &str,
        description: Option<&str>,
    ) -> Result<(), Error> {
        let metadata = self.community_metadata(jid).await?;
        let id = if description.is_some_and(|s| !s.is_empty()) {
            crate::messages::generate_id()?
        } else {
            String::new()
        };
        self.group_query(
            jid,
            QueryType::Set,
            vec![groups::description_node(
                description,
                metadata.desc_id.as_deref(),
                &id,
            )],
        )
        .await?;
        Ok(())
    }
    pub async fn community_invite_code(&self, jid: &str) -> Result<Option<String>, Error> {
        self.group_invite_code(jid).await
    }
    pub async fn community_revoke_invite(&self, jid: &str) -> Result<Option<String>, Error> {
        self.group_revoke_invite(jid).await
    }
    pub async fn community_accept_invite(&self, code: &str) -> Result<Option<String>, Error> {
        let result = self
            .group_query(
                "@g.us",
                QueryType::Set,
                vec![BinaryNode::new("invite").attr("code", code)],
            )
            .await?;
        Ok(result
            .child("community")
            .and_then(|n| n.attrs.get("jid").cloned()))
    }
    /// Accept a V4 invite to a community; see [`Socket::group_accept_invite_v4`].
    pub async fn community_accept_invite_v4(
        &self,
        key: &crate::proto::MessageKey,
        invite: &crate::proto::message::GroupInviteMessage,
    ) -> Result<Option<String>, Error> {
        self.group_accept_invite_v4(key, invite).await
    }
    pub async fn community_revoke_invite_v4(
        &self,
        community_jid: &str,
        invited_jid: &str,
    ) -> Result<bool, Error> {
        self.group_revoke_invite_v4(community_jid, invited_jid)
            .await
    }
    pub async fn community_get_invite_info(&self, code: &str) -> Result<Metadata, Error> {
        let result = self
            .group_query(
                "@g.us",
                QueryType::Get,
                vec![BinaryNode::new("invite").attr("code", code)],
            )
            .await?;
        Ok(communities::extract_metadata(&result)?)
    }
    pub async fn community_toggle_ephemeral(
        &self,
        jid: &str,
        expiration: u64,
    ) -> Result<(), Error> {
        self.group_toggle_ephemeral(jid, expiration).await
    }
    pub async fn community_setting_update(&self, jid: &str, setting: Setting) -> Result<(), Error> {
        self.group_setting_update(jid, setting).await
    }
    pub async fn community_member_add_mode(
        &self,
        jid: &str,
        mode: MemberAddMode,
    ) -> Result<(), Error> {
        self.group_member_add_mode(jid, mode).await
    }
    pub async fn community_join_approval_mode(
        &self,
        jid: &str,
        mode: JoinApprovalMode,
    ) -> Result<(), Error> {
        self.group_query(
            jid,
            QueryType::Set,
            vec![
                BinaryNode::new("membership_approval_mode").with_content(NodeContent::Children(
                    vec![BinaryNode::new("community_join").attr(
                        "state",
                        match mode {
                            JoinApprovalMode::On => "on",
                            JoinApprovalMode::Off => "off",
                        },
                    )],
                )),
            ],
        )
        .await?;
        Ok(())
    }
    /// Refresh participating communities and acknowledge the dirty bit as upstream does.
    pub(super) async fn handle_communities_dirty(&self) -> Result<(), Error> {
        self.community_fetch_all_participating().await?;
        self.clean_dirty_bits("groups", None).await
    }
}
