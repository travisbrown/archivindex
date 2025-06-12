pub mod data;
pub mod flat;

pub trait TweetSnapshot {
    fn id(&self) -> u64;
    fn user_id(&self) -> u64;
    fn user_screen_name(&self) -> Option<&str>;

    fn canonical_url(&self, use_x: bool) -> Option<String> {
        self.user_screen_name().map(|screen_name| {
            format!(
                "https://{}.com/{}/status/{}",
                if use_x { "x" } else { "twitter" },
                screen_name,
                self.id()
            )
        })
    }
}

impl<'a> TweetSnapshot for data::TweetSnapshot<'a> {
    fn id(&self) -> u64 {
        self.data.id
    }

    fn user_id(&self) -> u64 {
        self.data.author_id
    }

    fn user_screen_name(&self) -> Option<&str> {
        self.lookup_user(self.user_id())
            .map(|user| user.username.as_ref())
    }
}

impl<'a> TweetSnapshot for flat::TweetSnapshot<'a> {
    fn id(&self) -> u64 {
        self.id_str
    }

    fn user_id(&self) -> u64 {
        self.user.id_str
    }

    fn user_screen_name(&self) -> Option<&str> {
        Some(&self.user.screen_name)
    }
}
