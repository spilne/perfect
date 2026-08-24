# Performance history

Last 4 run(s) of 4 recorded. Medians in each benchmark's own unit (ns/op or ns/item). Absolute values are only comparable within a runner class — the trend is the signal, not the number.

## core/all x100 run

latest **19.26** · window min 15.93 / max 20.19 · drift across window +4.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 18.38 |
| 2026-08-24 | `341bc80b` | 15.93 |
| 2026-08-24 | `6497f365` | 20.19 |
| 2026-08-24 | `0dc21388` | 19.26 |

## core/flatMap chain x10k runSync

latest **35.81** · window min 22.17 / max 35.81 · drift across window +61.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 22.17 |
| 2026-08-24 | `341bc80b` | 22.46 |
| 2026-08-24 | `6497f365` | 31.88 |
| 2026-08-24 | `0dc21388` | 35.81 |

## core/run(sync)

latest **330.62** · window min 318.44 / max 393.43 · drift across window -7.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 358.21 |
| 2026-08-24 | `341bc80b` | 318.44 |
| 2026-08-24 | `6497f365` | 393.43 |
| 2026-08-24 | `0dc21388` | 330.62 |

## core/runSync(succeed)

latest **31.66** · window min 23.94 / max 48.32 · drift across window -34.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 48.32 |
| 2026-08-24 | `341bc80b` | 23.94 |
| 2026-08-24 | `6497f365` | 34.53 |
| 2026-08-24 | `0dc21388` | 31.66 |

## core/stream map/filter/take

latest **3.83** · window min 3.83 / max 5.13 · drift across window -24.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 5.10 |
| 2026-08-24 | `341bc80b` | 5.13 |
| 2026-08-24 | `6497f365` | 4.43 |
| 2026-08-24 | `0dc21388` | 3.83 |

## http/GET @perfect/http client

latest **142020.00** · window min 98248.00 / max 193624.00 · drift across window -26.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 193624.00 |
| 2026-08-24 | `341bc80b` | 98248.00 |
| 2026-08-24 | `6497f365` | 112757.00 |
| 2026-08-24 | `0dc21388` | 142020.00 |

## http/GET @perfect/http httpRequestJson

latest **135380.00** · window min 108292.00 / max 179147.00 · drift across window -24.4%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 179147.00 |
| 2026-08-24 | `341bc80b` | 108292.00 |
| 2026-08-24 | `6497f365` | 114287.00 |
| 2026-08-24 | `0dc21388` | 135380.00 |

## http/GET axios

latest **394954.00** · window min 314412.00 / max 394954.00 · drift across window +0.4%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 393560.00 |
| 2026-08-24 | `341bc80b` | 314412.00 |
| 2026-08-24 | `6497f365` | 357537.00 |
| 2026-08-24 | `0dc21388` | 394954.00 |

## http/GET fetch (baseline)

latest **89282.00** · window min 70535.00 / max 152106.00 · drift across window -41.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 152106.00 |
| 2026-08-24 | `341bc80b` | 70535.00 |
| 2026-08-24 | `6497f365` | 82677.00 |
| 2026-08-24 | `0dc21388` | 89282.00 |

## http/GET node-fetch

latest **112747.00** · window min 87672.00 / max 172013.00 · drift across window -34.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 172013.00 |
| 2026-08-24 | `341bc80b` | 87672.00 |
| 2026-08-24 | `6497f365` | 96910.00 |
| 2026-08-24 | `0dc21388` | 112747.00 |

## http/GET undici.request

latest **96963.00** · window min 79340.00 / max 165187.00 · drift across window -41.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 165187.00 |
| 2026-08-24 | `341bc80b` | 79340.00 |
| 2026-08-24 | `6497f365` | 99054.00 |
| 2026-08-24 | `0dc21388` | 96963.00 |

## http/POST @perfect/http client

latest **172115.00** · window min 104747.00 / max 189256.00 · drift across window -9.1%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 189256.00 |
| 2026-08-24 | `341bc80b` | 104747.00 |
| 2026-08-24 | `6497f365` | 125414.00 |
| 2026-08-24 | `0dc21388` | 172115.00 |

## http/POST axios

latest **400663.00** · window min 317536.00 / max 400663.00 · drift across window +5.1%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 381226.00 |
| 2026-08-24 | `341bc80b` | 317536.00 |
| 2026-08-24 | `6497f365` | 352890.00 |
| 2026-08-24 | `0dc21388` | 400663.00 |

## http/POST fetch (baseline)

latest **108531.00** · window min 73811.00 / max 156574.00 · drift across window -30.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 156574.00 |
| 2026-08-24 | `341bc80b` | 73811.00 |
| 2026-08-24 | `6497f365` | 85915.00 |
| 2026-08-24 | `0dc21388` | 108531.00 |

