# Performance history

Last 7 run(s) of 7 recorded. Medians in each benchmark's own unit (ns/op or ns/item). Absolute values are only comparable within a runner class — the trend is the signal, not the number.

## core/all x100 run

latest **18.68** · window min 15.93 / max 20.19 · drift across window +1.6%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 18.38 |
| 2026-08-24 | `341bc80b` | 15.93 |
| 2026-08-24 | `6497f365` | 20.19 |
| 2026-08-24 | `0dc21388` | 19.26 |
| 2026-08-24 | `22e7fca6` | 19.36 |
| 2026-08-24 | `54856bca` | 19.27 |
| 2026-08-29 | `639fb51f` | 18.68 |

## core/flatMap chain x10k runSync

latest **23.48** · window min 22.17 / max 35.81 · drift across window +5.9%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 22.17 |
| 2026-08-24 | `341bc80b` | 22.46 |
| 2026-08-24 | `6497f365` | 31.88 |
| 2026-08-24 | `0dc21388` | 35.81 |
| 2026-08-24 | `22e7fca6` | 22.19 |
| 2026-08-24 | `54856bca` | 26.02 |
| 2026-08-29 | `639fb51f` | 23.48 |

## core/run(sync)

latest **354.70** · window min 318.44 / max 393.43 · drift across window -1.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 358.21 |
| 2026-08-24 | `341bc80b` | 318.44 |
| 2026-08-24 | `6497f365` | 393.43 |
| 2026-08-24 | `0dc21388` | 330.62 |
| 2026-08-24 | `22e7fca6` | 370.16 |
| 2026-08-24 | `54856bca` | 353.93 |
| 2026-08-29 | `639fb51f` | 354.70 |

## core/runSync(succeed)

latest **34.73** · window min 19.18 / max 48.32 · drift across window -28.1%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 48.32 |
| 2026-08-24 | `341bc80b` | 23.94 |
| 2026-08-24 | `6497f365` | 34.53 |
| 2026-08-24 | `0dc21388` | 31.66 |
| 2026-08-24 | `22e7fca6` | 19.18 |
| 2026-08-24 | `54856bca` | 33.42 |
| 2026-08-29 | `639fb51f` | 34.73 |

## core/stream map/filter/take

latest **5.29** · window min 3.83 / max 5.90 · drift across window +3.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 5.10 |
| 2026-08-24 | `341bc80b` | 5.13 |
| 2026-08-24 | `6497f365` | 4.43 |
| 2026-08-24 | `0dc21388` | 3.83 |
| 2026-08-24 | `22e7fca6` | 4.75 |
| 2026-08-24 | `54856bca` | 5.90 |
| 2026-08-29 | `639fb51f` | 5.29 |

## http/GET @perfect/http client

latest **118566.00** · window min 98248.00 / max 193624.00 · drift across window -38.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 193624.00 |
| 2026-08-24 | `341bc80b` | 98248.00 |
| 2026-08-24 | `6497f365` | 112757.00 |
| 2026-08-24 | `0dc21388` | 142020.00 |
| 2026-08-24 | `22e7fca6` | 165039.00 |
| 2026-08-24 | `54856bca` | 118566.00 |

## http/GET @perfect/http httpRequestJson

latest **135331.00** · window min 108292.00 / max 179147.00 · drift across window -24.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 179147.00 |
| 2026-08-24 | `341bc80b` | 108292.00 |
| 2026-08-24 | `6497f365` | 114287.00 |
| 2026-08-24 | `0dc21388` | 135380.00 |
| 2026-08-24 | `22e7fca6` | 167674.00 |
| 2026-08-24 | `54856bca` | 135331.00 |

## http/GET @spilne/perfect-http client

latest **147570.00** · window min 147570.00 / max 147570.00 · drift across window +0.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 147570.00 |

## http/GET @spilne/perfect-http httpRequestJson

latest **139547.00** · window min 139547.00 / max 139547.00 · drift across window +0.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 139547.00 |

## http/GET axios

latest **363461.00** · window min 314412.00 / max 394954.00 · drift across window -7.6%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 393560.00 |
| 2026-08-24 | `341bc80b` | 314412.00 |
| 2026-08-24 | `6497f365` | 357537.00 |
| 2026-08-24 | `0dc21388` | 394954.00 |
| 2026-08-24 | `22e7fca6` | 371435.00 |
| 2026-08-24 | `54856bca` | 357600.00 |
| 2026-08-29 | `639fb51f` | 363461.00 |

## http/GET fetch (baseline)

latest **85908.00** · window min 70535.00 / max 152106.00 · drift across window -43.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 152106.00 |
| 2026-08-24 | `341bc80b` | 70535.00 |
| 2026-08-24 | `6497f365` | 82677.00 |
| 2026-08-24 | `0dc21388` | 89282.00 |
| 2026-08-24 | `22e7fca6` | 124823.00 |
| 2026-08-24 | `54856bca` | 85717.00 |
| 2026-08-29 | `639fb51f` | 85908.00 |

## http/GET node-fetch

latest **109273.00** · window min 87672.00 / max 172013.00 · drift across window -36.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 172013.00 |
| 2026-08-24 | `341bc80b` | 87672.00 |
| 2026-08-24 | `6497f365` | 96910.00 |
| 2026-08-24 | `0dc21388` | 112747.00 |
| 2026-08-24 | `22e7fca6` | 123491.00 |
| 2026-08-24 | `54856bca` | 113388.00 |
| 2026-08-29 | `639fb51f` | 109273.00 |

## http/GET undici.request

latest **100280.00** · window min 79340.00 / max 165187.00 · drift across window -39.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 165187.00 |
| 2026-08-24 | `341bc80b` | 79340.00 |
| 2026-08-24 | `6497f365` | 99054.00 |
| 2026-08-24 | `0dc21388` | 96963.00 |
| 2026-08-24 | `22e7fca6` | 127569.00 |
| 2026-08-24 | `54856bca` | 100508.00 |
| 2026-08-29 | `639fb51f` | 100280.00 |

## http/POST @perfect/http client

latest **169561.00** · window min 104747.00 / max 189256.00 · drift across window -10.4%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 189256.00 |
| 2026-08-24 | `341bc80b` | 104747.00 |
| 2026-08-24 | `6497f365` | 125414.00 |
| 2026-08-24 | `0dc21388` | 172115.00 |
| 2026-08-24 | `22e7fca6` | 165760.00 |
| 2026-08-24 | `54856bca` | 169561.00 |

## http/POST @spilne/perfect-http client

latest **130114.00** · window min 130114.00 / max 130114.00 · drift across window +0.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 130114.00 |

## http/POST axios

latest **389941.00** · window min 317536.00 / max 402461.00 · drift across window +2.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 381226.00 |
| 2026-08-24 | `341bc80b` | 317536.00 |
| 2026-08-24 | `6497f365` | 352890.00 |
| 2026-08-24 | `0dc21388` | 400663.00 |
| 2026-08-24 | `22e7fca6` | 398005.00 |
| 2026-08-24 | `54856bca` | 402461.00 |
| 2026-08-29 | `639fb51f` | 389941.00 |

## http/POST fetch (baseline)

latest **91837.00** · window min 73811.00 / max 156574.00 · drift across window -41.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 156574.00 |
| 2026-08-24 | `341bc80b` | 73811.00 |
| 2026-08-24 | `6497f365` | 85915.00 |
| 2026-08-24 | `0dc21388` | 108531.00 |
| 2026-08-24 | `22e7fca6` | 152034.00 |
| 2026-08-24 | `54856bca` | 100218.00 |
| 2026-08-29 | `639fb51f` | 91837.00 |

