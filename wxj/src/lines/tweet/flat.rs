use birdsite::model::attributes::integer_str;
use std::borrow::Cow;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TweetSnapshot<'a> {
    #[serde(with = "integer_str")]
    pub id_str: u64,
    pub user: User<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct User<'a> {
    #[serde(with = "integer_str")]
    pub id_str: u64,
    pub screen_name: Cow<'a, str>,
}
