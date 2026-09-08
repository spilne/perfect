# Performance history

Last 10 run(s) of 10 recorded. Medians in each benchmark's own unit (ns/op or ns/item). Absolute values are only comparable within a runner class — the trend is the signal, not the number.

## core/all x100 run

latest **18.93** · window min 15.93 / max 20.19 · drift across window +3.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 18.38 |
| 2026-08-24 | `341bc80b` | 15.93 |
| 2026-08-24 | `6497f365` | 20.19 |
| 2026-08-24 | `0dc21388` | 19.26 |
| 2026-08-24 | `22e7fca6` | 19.36 |
| 2026-08-24 | `54856bca` | 19.27 |
| 2026-08-29 | `639fb51f` | 18.68 |
| 2026-09-05 | `bf81003e` | 18.93 |

## core/all(succeed) x100 fast path

latest **15.67** · window min 14.63 / max 15.67 · drift across window +7.1%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 14.63 |
| 2026-09-08 | `0bc5aca4` | 15.67 |

## core/all(sync) x100 fibers

latest **541.01** · window min 541.01 / max 582.49 · drift across window -7.1%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 582.49 |
| 2026-09-08 | `0bc5aca4` | 541.01 |

## core/all(yieldNow) x100 fibers

latest **480.54** · window min 470.55 / max 480.54 · drift across window +2.1%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 470.55 |
| 2026-09-08 | `0bc5aca4` | 480.54 |

## core/deferred waiter cancellation x1000

latest **267.69** · window min 252.12 / max 267.69 · drift across window +6.2%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 252.12 |
| 2026-09-08 | `0bc5aca4` | 267.69 |

## core/deferred waiter cancellation x8000

latest **287.40** · window min 287.40 / max 295.29 · drift across window -2.7%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 295.29 |
| 2026-09-08 | `0bc5aca4` | 287.40 |

## core/fiber reverse completion x1000

latest **105.93** · window min 99.03 / max 105.93 · drift across window +7.0%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 99.03 |
| 2026-09-08 | `0bc5aca4` | 105.93 |

## core/fiber reverse completion x8000

latest **111.03** · window min 103.23 / max 111.03 · drift across window +7.6%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 103.23 |
| 2026-09-08 | `0bc5aca4` | 111.03 |

## core/flatMap chain x10k runSync

latest **28.31** · window min 22.17 / max 35.81 · drift across window +27.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 22.17 |
| 2026-08-24 | `341bc80b` | 22.46 |
| 2026-08-24 | `6497f365` | 31.88 |
| 2026-08-24 | `0dc21388` | 35.81 |
| 2026-08-24 | `22e7fca6` | 22.19 |
| 2026-08-24 | `54856bca` | 26.02 |
| 2026-08-29 | `639fb51f` | 23.48 |
| 2026-09-05 | `bf81003e` | 23.76 |
| 2026-09-06 | `4dd99df6` | 27.38 |
| 2026-09-08 | `0bc5aca4` | 28.31 |

## core/group singleton chunks x1000

latest **191.90** · window min 175.20 / max 191.90 · drift across window +9.5%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 175.20 |
| 2026-09-08 | `0bc5aca4` | 191.90 |

## core/group singleton chunks x8000

latest **370.30** · window min 338.55 / max 370.30 · drift across window +9.4%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 338.55 |
| 2026-09-08 | `0bc5aca4` | 370.30 |

## core/range construction x10000

latest **31.56** · window min 31.29 / max 31.56 · drift across window +0.9%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 31.29 |
| 2026-09-08 | `0bc5aca4` | 31.56 |

## core/range construction x1000000

latest **32.00** · window min 29.91 / max 32.00 · drift across window +7.0%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 29.91 |
| 2026-09-08 | `0bc5aca4` | 32.00 |

## core/range take(1) x10000

latest **6022.15** · window min 5710.04 / max 6022.15 · drift across window +5.5%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 5710.04 |
| 2026-09-08 | `0bc5aca4` | 6022.15 |

## core/range take(1) x1000000

latest **6009.53** · window min 5705.21 / max 6009.53 · drift across window +5.3%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 5705.21 |
| 2026-09-08 | `0bc5aca4` | 6009.53 |

## core/run(sync)

latest **319.63** · window min 280.90 / max 393.43 · drift across window -10.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 358.21 |
| 2026-08-24 | `341bc80b` | 318.44 |
| 2026-08-24 | `6497f365` | 393.43 |
| 2026-08-24 | `0dc21388` | 330.62 |
| 2026-08-24 | `22e7fca6` | 370.16 |
| 2026-08-24 | `54856bca` | 353.93 |
| 2026-08-29 | `639fb51f` | 354.70 |
| 2026-09-05 | `bf81003e` | 372.01 |
| 2026-09-06 | `4dd99df6` | 280.90 |
| 2026-09-08 | `0bc5aca4` | 319.63 |

## core/runSync(succeed)

latest **15.45** · window min 15.45 / max 48.32 · drift across window -68.0%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 48.32 |
| 2026-08-24 | `341bc80b` | 23.94 |
| 2026-08-24 | `6497f365` | 34.53 |
| 2026-08-24 | `0dc21388` | 31.66 |
| 2026-08-24 | `22e7fca6` | 19.18 |
| 2026-08-24 | `54856bca` | 33.42 |
| 2026-08-29 | `639fb51f` | 34.73 |
| 2026-09-05 | `bf81003e` | 27.53 |
| 2026-09-06 | `4dd99df6` | 21.88 |
| 2026-09-08 | `0bc5aca4` | 15.45 |

## core/sliding window fill x1000

latest **472.71** · window min 452.03 / max 472.71 · drift across window +4.6%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 452.03 |
| 2026-09-08 | `0bc5aca4` | 472.71 |

## core/sliding window fill x8000

latest **475.87** · window min 471.44 / max 475.87 · drift across window +0.9%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 471.44 |
| 2026-09-08 | `0bc5aca4` | 475.87 |

## core/stream map/filter full traversal

latest **11.60** · window min 8.81 / max 11.60 · drift across window +31.7%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 8.81 |
| 2026-09-08 | `0bc5aca4` | 11.60 |

## core/stream map/filter/take

latest **6.36** · window min 3.83 / max 6.36 · drift across window +24.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 5.10 |
| 2026-08-24 | `341bc80b` | 5.13 |
| 2026-08-24 | `6497f365` | 4.43 |
| 2026-08-24 | `0dc21388` | 3.83 |
| 2026-08-24 | `22e7fca6` | 4.75 |
| 2026-08-24 | `54856bca` | 5.90 |
| 2026-08-29 | `639fb51f` | 5.29 |
| 2026-09-05 | `bf81003e` | 6.36 |

## core/stream map/filter/take end-to-end

latest **125435.00** · window min 72546.00 / max 125435.00 · drift across window +72.9%

| run | commit | median |
|---:|---|---:|
| 2026-09-06 | `4dd99df6` | 72546.00 |
| 2026-09-08 | `0bc5aca4` | 125435.00 |

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

latest **189434.00** · window min 139107.00 / max 189434.00 · drift across window +28.4%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 147570.00 |
| 2026-09-05 | `bf81003e` | 139107.00 |
| 2026-09-06 | `4dd99df6` | 188393.00 |
| 2026-09-08 | `0bc5aca4` | 189434.00 |

## http/GET @spilne/perfect-http httpRequestJson

latest **184726.00** · window min 139547.00 / max 184726.00 · drift across window +32.4%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 139547.00 |
| 2026-09-05 | `bf81003e` | 146277.00 |
| 2026-09-06 | `4dd99df6` | 172352.00 |
| 2026-09-08 | `0bc5aca4` | 184726.00 |

## http/GET axios

latest **362337.00** · window min 314412.00 / max 409586.00 · drift across window -7.9%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 393560.00 |
| 2026-08-24 | `341bc80b` | 314412.00 |
| 2026-08-24 | `6497f365` | 357537.00 |
| 2026-08-24 | `0dc21388` | 394954.00 |
| 2026-08-24 | `22e7fca6` | 371435.00 |
| 2026-08-24 | `54856bca` | 357600.00 |
| 2026-08-29 | `639fb51f` | 363461.00 |
| 2026-09-05 | `bf81003e` | 390189.00 |
| 2026-09-06 | `4dd99df6` | 409586.00 |
| 2026-09-08 | `0bc5aca4` | 362337.00 |

## http/GET fetch (baseline)

latest **142246.00** · window min 70535.00 / max 152106.00 · drift across window -6.5%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 152106.00 |
| 2026-08-24 | `341bc80b` | 70535.00 |
| 2026-08-24 | `6497f365` | 82677.00 |
| 2026-08-24 | `0dc21388` | 89282.00 |
| 2026-08-24 | `22e7fca6` | 124823.00 |
| 2026-08-24 | `54856bca` | 85717.00 |
| 2026-08-29 | `639fb51f` | 85908.00 |
| 2026-09-05 | `bf81003e` | 87090.00 |
| 2026-09-06 | `4dd99df6` | 121026.00 |
| 2026-09-08 | `0bc5aca4` | 142246.00 |

## http/GET node-fetch

latest **131095.00** · window min 87672.00 / max 172013.00 · drift across window -23.8%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 172013.00 |
| 2026-08-24 | `341bc80b` | 87672.00 |
| 2026-08-24 | `6497f365` | 96910.00 |
| 2026-08-24 | `0dc21388` | 112747.00 |
| 2026-08-24 | `22e7fca6` | 123491.00 |
| 2026-08-24 | `54856bca` | 113388.00 |
| 2026-08-29 | `639fb51f` | 109273.00 |
| 2026-09-05 | `bf81003e` | 106408.00 |
| 2026-09-06 | `4dd99df6` | 153597.00 |
| 2026-09-08 | `0bc5aca4` | 131095.00 |

## http/GET undici.request

latest **162524.00** · window min 79340.00 / max 165187.00 · drift across window -1.6%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 165187.00 |
| 2026-08-24 | `341bc80b` | 79340.00 |
| 2026-08-24 | `6497f365` | 99054.00 |
| 2026-08-24 | `0dc21388` | 96963.00 |
| 2026-08-24 | `22e7fca6` | 127569.00 |
| 2026-08-24 | `54856bca` | 100508.00 |
| 2026-08-29 | `639fb51f` | 100280.00 |
| 2026-09-05 | `bf81003e` | 147208.00 |
| 2026-09-06 | `4dd99df6` | 123231.00 |
| 2026-09-08 | `0bc5aca4` | 162524.00 |

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

latest **169497.00** · window min 130114.00 / max 173325.00 · drift across window +30.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-29 | `639fb51f` | 130114.00 |
| 2026-09-05 | `bf81003e` | 136753.00 |
| 2026-09-06 | `4dd99df6` | 173325.00 |
| 2026-09-08 | `0bc5aca4` | 169497.00 |

## http/POST axios

latest **429533.00** · window min 317536.00 / max 444500.00 · drift across window +12.7%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 381226.00 |
| 2026-08-24 | `341bc80b` | 317536.00 |
| 2026-08-24 | `6497f365` | 352890.00 |
| 2026-08-24 | `0dc21388` | 400663.00 |
| 2026-08-24 | `22e7fca6` | 398005.00 |
| 2026-08-24 | `54856bca` | 402461.00 |
| 2026-08-29 | `639fb51f` | 389941.00 |
| 2026-09-05 | `bf81003e` | 444500.00 |
| 2026-09-06 | `4dd99df6` | 442808.00 |
| 2026-09-08 | `0bc5aca4` | 429533.00 |

## http/POST fetch (baseline)

latest **123180.00** · window min 73811.00 / max 156574.00 · drift across window -21.3%

| run | commit | median |
|---:|---|---:|
| 2026-08-23 | `4b3a1fd6` | 156574.00 |
| 2026-08-24 | `341bc80b` | 73811.00 |
| 2026-08-24 | `6497f365` | 85915.00 |
| 2026-08-24 | `0dc21388` | 108531.00 |
| 2026-08-24 | `22e7fca6` | 152034.00 |
| 2026-08-24 | `54856bca` | 100218.00 |
| 2026-08-29 | `639fb51f` | 91837.00 |
| 2026-09-05 | `bf81003e` | 89733.00 |
| 2026-09-06 | `4dd99df6` | 121658.00 |
| 2026-09-08 | `0bc5aca4` | 123180.00 |

