use birdsite::model::attributes::integer_str;
use std::borrow::Cow;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TweetSnapshot<'a> {
    pub data: Data,
    pub includes: Includes<'a>,
    pub errors: Option<Vec<Error>>,
}

impl<'a> TweetSnapshot<'a> {
    pub fn lookup_user(&self, id: u64) -> Option<&User<'a>> {
        self.includes.users.iter().find(|user| user.id == id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Data {
    #[serde(with = "integer_str")]
    pub id: u64,
    #[serde(with = "integer_str")]
    pub author_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Includes<'a> {
    pub tweets: Option<Vec<Tweet>>,
    pub users: Vec<User<'a>>,
    pub media: Option<Vec<Media>>,
    pub polls: Option<Vec<Poll>>,
    pub places: Option<Vec<Place>>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Tweet {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct User<'a> {
    #[serde(with = "integer_str")]
    pub id: u64,
    pub username: Cow<'a, str>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Media {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Poll {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Place {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Error {}
