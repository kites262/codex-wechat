use codex_wechat::wechat::{Account, WechatApiError, WechatClient};

fn account() -> Account {
    Account {
        bot_id: "bot".into(),
        owner_id: "owner".into(),
        token: "synthetic-credential".into(),
        base_url: "https://ilinkai.weixin.qq.com".into(),
    }
}

#[test]
fn account_keeps_existing_camel_case_state_contract() {
    let value = serde_json::to_value(account()).unwrap();
    assert_eq!(value["botId"], "bot");
    assert_eq!(value["ownerId"], "owner");
    assert_eq!(value["baseUrl"], "https://ilinkai.weixin.qq.com");
    let decoded: Account = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.token, "synthetic-credential");
}

#[test]
fn credentials_are_never_accepted_for_untrusted_origins() {
    for origin in [
        "http://ilinkai.weixin.qq.com",
        "https://evil.test",
        "https://ilinkai.weixin.qq.com.evil.test",
        "https://user@ilinkai.weixin.qq.com",
        "https://ilinkai.weixin.qq.com/path",
        "https://ilinkai.weixin.qq.com:8443",
        "https://ilinkai.weixin.qq.com/?query=1",
        "https://ilinkai.weixin.qq.com/#fragment",
    ] {
        let mut a = account();
        a.base_url = origin.into();
        assert!(WechatClient::new(a).is_err(), "{origin}");
    }
    let mut a = account();
    a.token.clear();
    assert!(WechatClient::new(a).is_err());
}

#[test]
fn expired_credentials_are_distinguishable_from_retryable_api_errors() {
    assert!(WechatApiError { code: -14 }.requires_login());
    assert!(!WechatApiError { code: -1 }.requires_login());
    assert!(
        WechatApiError { code: -14 }
            .to_string()
            .contains("run login, then restart")
    );
}
