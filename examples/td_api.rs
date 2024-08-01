use std::borrow::Cow;
use std::ffi::CString;
use std::fs;
use std::time::Duration;

use futures::StreamExt;
use tracing::{debug, info};
use tracing::subscriber::set_global_default;
use tracing_subscriber::EnvFilter;

use ctp_sys::{CThostFtdcReqAuthenticateField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField, CtpAccountConfig, gb18030_cstr_to_str_i8, set_cstr_from_str_truncate_i8, THOST_TE_RESUME_TYPE_THOST_TERT_QUICK};
use ctp_sys::trader_api::{create_api, create_spi};

#[tokio::main]
async fn main() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(env_filter)
        .with_level(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .finish();
    set_global_default(subscriber)
        .expect("failed to set up global tracing subscriber");

    let account = CtpAccountConfig::from_env();
    trader(&account).await;
}


fn check_make_dir(path: &String) {
    // 创建目录
    match fs::create_dir_all(path) {
        Ok(_) => info!("目录创建成功：{}", path),
        Err(e) => info!("无法创建目录：{}->{}", path, e),
    }
}

async fn trader(ca: &CtpAccountConfig) {
    let address = ca.trade_front.as_str();
    let broker_id = ca.broker_id.as_str();
    let user_id = ca.account.as_str();
    let password = ca.password.as_str();
    let app_id = ca.app_id.as_str();
    let auth_code = ca.auth_code.as_str();

    let mut request_id = 0;
    let mut get_request_id = || {
        request_id += 1;
        request_id
    };

    let flow_path = format!(".cache/ctp_futures_md_flow_{}_{}/", broker_id, user_id);
    check_make_dir(&flow_path);

    let mut api = create_api(&flow_path, false);
    api.register_front(CString::new(address).unwrap());
    let mut stream = {
        let (stream, spi) = create_spi();
        api.register_spi(spi);
        stream
    };
    info!("register front {}", address);
    api.subscribe_private_topic(THOST_TE_RESUME_TYPE_THOST_TERT_QUICK);
    api.subscribe_public_topic(THOST_TE_RESUME_TYPE_THOST_TERT_QUICK);
    api.init();
    info!("api inited");

    let trading_day = api.get_trading_day();
    info!("trading_day: {:?}", trading_day);

    while let Some(msg) = stream.next().await {
        use ctp_sys::trader_api::CThostFtdcTraderSpiOutput::*;
        match msg {
            OnFrontConnected(_) => {
                let mut req = CThostFtdcReqAuthenticateField::default();
                set_cstr_from_str_truncate_i8(&mut req.BrokerID, broker_id);
                set_cstr_from_str_truncate_i8(&mut req.UserID, user_id);
                set_cstr_from_str_truncate_i8(&mut req.AppID, app_id);
                set_cstr_from_str_truncate_i8(&mut req.AuthCode, auth_code);
                debug!("auth with: broker_id:{} user_id:{} app_id:{} auth_code:{}",
                    broker_id, user_id, app_id, auth_code);
                api.req_authenticate(&mut req, get_request_id());
            }
            OnFrontDisconnected(p) => {
                info!("front disconnected: {:?}", p)
            }
            OnRspAuthenticate(p) => {
                match parse_rsp_info(p.p_rsp_info) {
                    (0, _) => {
                        info!("auth success");
                        let mut req = CThostFtdcReqUserLoginField::default();
                        set_cstr_from_str_truncate_i8(&mut req.BrokerID, broker_id);
                        set_cstr_from_str_truncate_i8(&mut req.UserID, user_id);
                        set_cstr_from_str_truncate_i8(&mut req.Password, password);
                        let mut sys_info = [0; 273];
                        debug!("login with: broker_id:{} user_id:{}",broker_id, user_id);
                        api.req_user_login(&mut req, get_request_id(), 0, &mut sys_info);
                    }
                    (err_id, err_msg) => {
                        info!("auth fail, err_id: {} err_msg: {}", err_id, err_msg)
                    }
                }
            }
            OnRspUserLogin(p) => {
                match parse_rsp_info(p.p_rsp_info) {
                    (0, _) => {
                        info!("login success");
                    }
                    (err_id, err_msg) => {
                        info!("login fail: err_id: {} err_msg: {}", err_id, err_msg);
                    }
                }
            }
            _ => {}
        }
    }
    api.release();
    api.join();

    tokio::time::sleep(Duration::from_secs(100)).await;
    info!("finished")
}


fn parse_rsp_info(rsp: Option<CThostFtdcRspInfoField>) -> (i32, String) {
    if let Some(ref rsp) = rsp {
        (rsp.ErrorID as i32, gb18030_cstr_to_str_i8(&rsp.ErrorMsg).to_string())
    } else {
        (0, Cow::from("").to_string())
    }
}