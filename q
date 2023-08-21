[1mdiff --git a/package.json b/package.json[m
[1mindex e57cef7..5fc73f7 100644[m
[1m--- a/package.json[m
[1m+++ b/package.json[m
[36m@@ -49,25 +49,25 @@[m
     "snapshot": "npx standard-version --no-verify --skip.changelog --prerelease snapshot --releaseCommitMessageFormat 'chore(snapshot): {{currentTag}}'"[m
   },[m
   "dependencies": {[m
[31m-    "async": "3.2.3",[m
[32m+[m[32m    "async": "3.2.4",[m
     "events": "3.3.0",[m
[31m-    "node-rdkafka": "2.12.0"[m
[32m+[m[32m    "node-rdkafka": "2.17.0"[m
   },[m
   "devDependencies": {[m
     "audit-ci": "^6.6.1",[m
[31m-    "npm-check-updates": "13.0.1",[m
[32m+[m[32m    "npm-check-updates": "16.12.2",[m
     "nyc": "15.1.0",[m
     "pre-commit": "1.2.2",[m
[31m-    "replace": "^1.2.1",[m
[31m-    "rewire": "6.0.0",[m
[31m-    "sinon": "14.0.0",[m
[31m-    "standard": "17.0.0",[m
[32m+[m[32m    "replace": "^1.2.2",[m
[32m+[m[32m    "rewire": "7.0.0",[m
[32m+[m[32m    "sinon": "15.2.0",[m
[32m+[m[32m    "standard": "17.1.0",[m
     "standard-version": "^9.5.0",[m
     "tap-spec": "^5.0.0",[m
     "tap-xunit": "2.4.1",[m
[31m-    "tape": "4.13.3",[m
[32m+[m[32m    "tape": "5.6.6",[m
     "tapes": "4.1.0",[m
[31m-    "uuid4": "2.0.2"[m
[32m+[m[32m    "uuid4": "2.0.3"[m
   },[m
   "peerDependencies": {[m
     "@mojaloop/central-services-error-handling": "12.x.x",[m
