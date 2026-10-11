use super::{Error, Event, Socket};
use crate::{
    binary::{BinaryNode, NodeContent},
    groups::{
        self, JoinApprovalMode, MemberAddMode, Metadata, ParticipantAction, ParticipantResult,
        QueryType, RequestAction, Setting,
    },
};
use indexmap::IndexMap;
impl Socket {
    pub async fn group_query(
        &self,
        jid: &str,
        kind: QueryType,
        content: Vec<BinaryNode>,
    ) -> Result<BinaryNode, Error> {
        self.query(groups::query(jid, kind, content), None).await
    }
    pub async fn group_metadata(&self, jid: &str) -> Result<Metadata, Error> {
        let result = self
            .group_query(
                jid,
                QueryType::Get,
                vec![BinaryNode::new("query").attr("request", "interactive")],
            )
            .await?;
        Ok(groups::extract_metadata(&result)?)
    }
    pub async fn group_fetch_all_participating(&self) -> Result<IndexMap<String, Metadata>, Error> {
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
        let metadata = groups::extract_participating(&result)?;
        let _ = self
            .inner
            .events
            .send(Event::GroupsUpdated(metadata.values().cloned().collect()));
        Ok(metadata)
    }
    pub async fn group_create(
        &self,
        subject: &str,
        participants: &[String],
    ) -> Result<Metadata, Error> {
        let id = crate::messages::generate_id_v2(None)?;
        let result = self
            .group_query(
                "@g.us",
                QueryType::Set,
                vec![
                    BinaryNode::new("create")
                        .attr("subject", subject)
                        .attr("key", id)
                        .with_content(NodeContent::Children(groups::participant_nodes(
                            participants,
                        ))),
                ],
            )
            .await?;
        Ok(groups::extract_metadata(&result)?)
    }
    pub async fn group_leave(&self, id: &str) -> Result<(), Error> {
        self.group_query(
            "@g.us",
            QueryType::Set,
            vec![
                BinaryNode::new("leave").with_content(NodeContent::Children(vec![
                    BinaryNode::new("group").attr("id", id),
                ])),
            ],
        )
        .await?;
        Ok(())
    }
    pub async fn group_update_subject(&self, jid: &str, subject: &str) -> Result<(), Error> {
        self.group_query(
            jid,
            QueryType::Set,
            vec![
                BinaryNode::new("subject")
                    .with_content(NodeContent::Bytes(subject.as_bytes().to_vec())),
            ],
        )
        .await?;
        Ok(())
    }
    pub async fn group_request_participants_list(
        &self,
        jid: &str,
    ) -> Result<Vec<IndexMap<String, String>>, Error> {
        let result = self
            .group_query(
                jid,
                QueryType::Get,
                vec![BinaryNode::new("membership_approval_requests")],
            )
            .await?;
        Ok(result
            .child("membership_approval_requests")
            .into_iter()
            .flat_map(|n| n.children("membership_approval_request"))
            .map(|n| n.attrs.clone())
            .collect())
    }
    pub async fn group_request_participants_update(
        &self,
        jid: &str,
        participants: &[String],
        action: RequestAction,
    ) -> Result<Vec<ParticipantResult>, Error> {
        let result = self
            .group_query(
                jid,
                QueryType::Set,
                vec![BinaryNode::new("membership_requests_action").with_content(
                    NodeContent::Children(vec![BinaryNode::new(action.tag()).with_content(
                        NodeContent::Children(groups::participant_nodes(participants)),
                    )]),
                )],
            )
            .await?;
        Ok(groups::participant_results(
            result
                .child("membership_requests_action")
                .and_then(|n| n.child(action.tag())),
            false,
        ))
    }
    pub async fn group_participants_update(
        &self,
        jid: &str,
        participants: &[String],
        action: ParticipantAction,
    ) -> Result<Vec<ParticipantResult>, Error> {
        let result = self
            .group_query(
                jid,
                QueryType::Set,
                vec![
                    BinaryNode::new(action.tag()).with_content(NodeContent::Children(
                        groups::participant_nodes(participants),
                    )),
                ],
            )
            .await?;
        Ok(groups::participant_results(
            result.child(action.tag()),
            true,
        ))
    }
    pub async fn group_update_description(
        &self,
        jid: &str,
        description: Option<&str>,
    ) -> Result<(), Error> {
        let metadata = self.group_metadata(jid).await?;
        let id = if description.is_some_and(|s| !s.is_empty()) {
            crate::messages::generate_id_v2(None)?
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
    pub async fn group_invite_code(&self, jid: &str) -> Result<Option<String>, Error> {
        let result = self
            .group_query(jid, QueryType::Get, vec![BinaryNode::new("invite")])
            .await?;
        Ok(result
            .child("invite")
            .and_then(|n| n.attrs.get("code"))
            .cloned())
    }
    pub async fn group_revoke_invite(&self, jid: &str) -> Result<Option<String>, Error> {
        let result = self
            .group_query(jid, QueryType::Set, vec![BinaryNode::new("invite")])
            .await?;
        Ok(result
            .child("invite")
            .and_then(|n| n.attrs.get("code"))
            .cloned())
    }
    pub async fn group_accept_invite(&self, code: &str) -> Result<Option<String>, Error> {
        let result = self
            .group_query(
                "@g.us",
                QueryType::Set,
                vec![BinaryNode::new("invite").attr("code", code)],
            )
            .await?;
        Ok(result
            .child("group")
            .and_then(|n| n.attrs.get("jid"))
            .cloned())
    }
    pub async fn group_revoke_invite_v4(
        &self,
        group_jid: &str,
        invited_jid: &str,
    ) -> Result<bool, Error> {
        self.group_query(
            group_jid,
            QueryType::Set,
            vec![
                BinaryNode::new("revoke").with_content(NodeContent::Children(vec![
                    BinaryNode::new("participant").attr("jid", invited_jid),
                ])),
            ],
        )
        .await?;
        Ok(true)
    }
    pub async fn group_get_invite_info(&self, code: &str) -> Result<Metadata, Error> {
        let result = self
            .group_query(
                "@g.us",
                QueryType::Get,
                vec![BinaryNode::new("invite").attr("code", code)],
            )
            .await?;
        Ok(groups::extract_metadata(&result)?)
    }
    pub async fn group_toggle_ephemeral(&self, jid: &str, expiration: u64) -> Result<(), Error> {
        let node = if expiration == 0 {
            BinaryNode::new("not_ephemeral")
        } else {
            BinaryNode::new("ephemeral").attr("expiration", expiration.to_string())
        };
        self.group_query(jid, QueryType::Set, vec![node]).await?;
        Ok(())
    }
    pub async fn group_setting_update(&self, jid: &str, setting: Setting) -> Result<(), Error> {
        self.group_query(jid, QueryType::Set, vec![BinaryNode::new(setting.tag())])
            .await?;
        Ok(())
    }
    pub async fn group_member_add_mode(&self, jid: &str, mode: MemberAddMode) -> Result<(), Error> {
        self.group_query(
            jid,
            QueryType::Set,
            vec![
                BinaryNode::new("member_add_mode")
                    .with_content(NodeContent::Text(mode.text().into())),
            ],
        )
        .await?;
        Ok(())
    }
    pub async fn group_join_approval_mode(
        &self,
        jid: &str,
        mode: JoinApprovalMode,
    ) -> Result<(), Error> {
        self.group_query(
            jid,
            QueryType::Set,
            vec![
                BinaryNode::new("membership_approval_mode").with_content(NodeContent::Children(
                    vec![BinaryNode::new("group_join").attr("state", mode.text())],
                )),
            ],
        )
        .await?;
        Ok(())
    }
}
