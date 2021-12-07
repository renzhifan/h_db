<?php

namespace Hhz\DB;

interface ApiCode
{
    const API_CODE_SUCCES = 1;//接口请求正常
    const API_CODE_ERROR = 0;//接口请求异常 可预测失败
    const API_CODE_LOGIN_EXPIRE = 2;//登录身份过期
    const API_CODE_LOGIN_KICK = 201;//登录身份过期 被顶
    const API_CODE_EXCEPTION = 5;//接口异常 不可预测失败
    const API_CODE_DECRYPT_EXCEPTION = 10;//接口解密异常
    const API_SAFE_CHECK_ERROR = 11;//接口安全校验异常
    const API_CODE_NOT_REAL_NAME = 101;//需要实名
    const API_CODE_NEED_TOKEN = 102;//需要身份信息
    const API_CODE_NEED_LOGIN = 103;//需要登录
    const API_CODE_NEED_CODE = 104;//需要验证码登录
}
