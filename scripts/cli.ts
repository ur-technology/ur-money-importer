/// <reference path="../typings/index.d.ts" />

import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as moment from 'moment';

var log = {
  info: console.log,
  debug: console.log,
  trace: console.log,
  setDefaultLevel: (x) => { }
}; // couldn't get loglevel to work, so faking it this way

require('dotenv').config();

log.info(`starting with NODE_ENV ${process.env.NODE_ENV} and FIREBASE_PROJECT_ID ${process.env.FIREBASE_PROJECT_ID}`);

firebase.initializeApp({
  serviceAccount: `./serviceAccountCredentials.${process.env.FIREBASE_PROJECT_ID}.json`,
  databaseURL: `https://${process.env.FIREBASE_PROJECT_ID}.firebaseio.com`
});

let ref = (path) => {
  return firebase.database().ref(path);
}

let usersRef = () => {
  return ref(`/users`);
};

let userRef = (u) => {
  return usersRef().child(u.userId);
};

let hasSponsor = (u) => {
  return !!getSponsor(u);
};

let getSponsor = (u) => {
  return users[u.sponsor && u.sponsor.userId];
};

let directReferrals = (u) => {
    return _.filter(users, (x) => { return x.sponsor && x.sponsor.userId === u.userId; });
};

let getUserStatus;
getUserStatus = (user) => {
  let status = _.trim((user.registration && user.registration.status) || '');
  if (status !== 'announcement-confirmed' && status !== 'announcement-initiated')  {
    if (user.wallet && user.wallet.address) {
     let sponsor = getSponsor(user);
      if (user.disabled) {
        status = 'disabled';
      } else if (!sponsor || sponsor.disabled || !sponsor.wallet ||
        !sponsor.wallet.announcementTransaction || !sponsor.wallet.announcementTransaction.blockNumber) {
        status = 'waiting-on-upline';
      } else {
        status = 'ready-to-confirm';
      }
    } else {
      status = 'needs-wallet';
    }
  }
  return status;
}

let userGroups = (sampleUsers) => {
  sampleUsers = sampleUsers || users;
  return _.groupBy(sampleUsers, (u) => {
    let sponsorConfirmed = !u.sponsor || (!!users[u.sponsor.userId] && !!users[u.sponsor.userId].wallet && !!users[u.sponsor.userId].wallet.announcementTransaction && !!users[u.sponsor.userId].wallet.announcementTransaction.blockNumber);
   return getUserStatus(u) + ` - sponsor ${ sponsorConfirmed ? '' : 'not' } confirmed`;
 });
}

let initializeMetaData = (u: any) => {
  u.d = {
    potentialUrRewards: 0,
    numDirectReferrals: 0,
    numDownlineUsers: 0
  };
};

let roundNumbers = (u: any) => {
  u.d.potentialUrRewards = Math.round(u.d.potentialUrRewards);
};

let addDownlineInfoToUpline = (u) => {
  let status = (u.registration && u.registration.status) || 'initial';
  let bonuses = [60.60,60.60,121.21,181.81,303.03,484.84,787.91];

  let targetUser = u;
  for (let level = 0; level < 7; level++) {
    targetUser = targetUser.sponsor && targetUser.sponsor.userId && users[targetUser.sponsor.userId];
    if (!targetUser) {
      break;
    }
    if (level === 0) {
      targetUser.d.numDirectReferrals = (targetUser.d.numDirectReferrals || 0) + 1;
    }
    targetUser.d.numDownlineUsers = (targetUser.d.numDownlineUsers || 0) + 1;
    if (!u.disabled && status !== 'announcement-confirmed') {
      targetUser.d.potentialUrRewards = (targetUser.d.potentialUrRewards || 0) + bonuses[level];
    }
  }
}

let hasWallet = (u: any): boolean => {
  return !!u.wallet && !!u.wallet.address;
}

let setSponsorToEiland = (u) => {
  let eiland = _.find(users, {email: 'eiland@ur.technology'});
  let sponsorInfo = _.pick(eiland, ['name', 'profilePhotoUrl', 'userId']);
  sponsorInfo.announcementTransactionConfirmed = true;
  userRef(u).update({sponsor: sponsorInfo});
}

let fixOrphans = () => {
  let orphans = _.filter(users, (u) => { return u.sponsor && u.sponsor.userId === unknown.userId; });
  _.each(orphans, (o) => {
    let reportedSponsorName = o.prefineryUser && o.prefineryUser.userReportedReferrer;
    let reportedSponsorLastName = _.last(reportedSponsorName.split(" "));
    let sponsors = _.filter(users, (x) => {
      return x.name && x.name.toLowerCase() === reportedSponsorName.toLowerCase() && x.userId !== o.userId;
    });
    if (_.size(sponsors) === 0) {
      let sponsors = _.filter(users, (x) => {
        return x.name && x.lastName.toLowerCase() === reportedSponsorLastName.toLowerCase() && x.userId !== o.userId;
      });
      // log.info(`${o.email} - ${o.name} sponsored by ${ sponsors[0].name} ?`);
    } else if (_.size(sponsors) === 1) {
      log.info(`${o.email} - ${o.name} sponsored by ${ sponsors[0].name} ?`);
    } else if (_.size(sponsors) > 1) {
      // log.info(`${o.email} - ${o.name} sponsored by ${ sponsors[0].name} ?`);
    }
  });
};

let blocker = (u) => {
  let sponsor = getSponsor(u);
  if (u.email === 'eiland@ur.technology' || u.email === 'jreitano@ur.technology' || !sponsor) {
    return undefined;
  }
  if (sponsor.wallet && sponsor.wallet.address && !sponsor.diabled) {
    return blocker(sponsor);
  } else {
    return sponsor;
  }
};

let isTopUser = (u) => {
   return u.email === 'jreitano@ur.technology';
}

let walletEnabled = (u) => {
  return hasWallet(u) && !u.disabled && (hasSponsor(u) || isTopUser(u));
};

let announcementAlreadyInitiated = (u: any): boolean => {
  return /^announcement-/.test((u.registration && u.registration.status) || '') ||
    (u.wallet.announcementTransaction && (u.wallet.announcementTransaction.blockNumber || u.wallet.announcementTransaction.hash))
};

let readyForAnnouncement = (u) => {
  return walletEnabled(u) && !announcementAlreadyInitiated(u);
}

let isBlocked = (u) => {
  return readyForAnnouncement(u) && !!blocker(u);
}

let newSponsor = (u) => {
  if (!isBlocked(u)) {
    return undefined;
  }

  let currentUser = u;
  while(true) {
    currentUser = getSponsor(currentUser);
    if (walletEnabled(currentUser) && !isBlocked(currentUser)) {
      return currentUser;
    }
  }
};

let blockedUsers = () => {
  return _.filter(users, isBlocked);
}

let isAnnounceable = (u) => {
  return readyForAnnouncement(u) && !blocker(u);
}

let bigBlockers = () => {
  let blockers = _.sortBy(
    _.filter(users, (u) => {
      return getUserStatus(u) === 'needs-wallet' && u.downlineSize > 0;
    }),
    'downlineSize'
  );
  return _.map(_.takeRight(blockers, 20), (u) => {
    return `${u.email || u.name} is blocking ${u.downlineSize} users`
  }).join('\n');
}
// bigBlockers();


let referralUsers = (u) => {
  return _.filter(users, (r) => {
    return r.sponsor &&
    r.sponsor.userId === u.userId &&
    r.userId !== u.userId
  });
}
// downlineUsers(a);

let downlineUsers = (u) => {
  let referrals = referralUsers(u);
  return referrals.concat(_.flatten(_.map(referrals, downlineUsers)));
}
// downlineUsers(a);

let convertPushIdToTimestamp = (id) => {
  var PUSH_CHARS = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
  id = id.substring(0,8);
  var timestamp = 0;
  for (var i=0; i < id.length; i++) {
    var c = id.charAt(i);
    timestamp = timestamp * 64 + PUSH_CHARS.indexOf(c);
  }
  return timestamp;
};

let phoneAuthTasks;
let clearOldPhoneAuthTasks = () => {
  let tasksRef = firebase.database().ref(`/phoneAuthQueue/tasks`);
  tasksRef.once('value', (snapshot) => {
    phoneAuthTasks = snapshot.val() || {};
    _.each(phoneAuthTasks, (t,tid) => { t.taskId = tid; });
    log.info('done loading phoneAuthTasks');
    log.info('grouped by state', _.groupBy(phoneAuthTasks, '_state'));
    log.info('grouped by error', _.groupBy(phoneAuthTasks, '_error_details.error'));

    let count = 0;
    let now = new Date().valueOf();
    _.each(phoneAuthTasks, (t) => {
      let createdAt = convertPushIdToTimestamp(t.taskId);
      if ((now - createdAt)/1000/60/60 > 0.5) {
        tasksRef.child(t.taskId).remove();
        count++;
      }
    });
    log.info(`removed ${count} phoneAuthTasks`);
  });
}
// clearOldPhoneAuthTasks();

let createdWithinWeeks = (u, w1, w2) => {
  let now = new Date().valueOf();
  let weeksAgoCreated = (now - u.createdAt)/1000/60/60/24/7;
  return w1 <= weeksAgoCreated && weeksAgoCreated < w2 && u.wallet && u.wallet.address;
}

let growth = (weeksBack) => {
  let sample = _.filter(users, (u) => { return createdWithinWeek(u, weeksBack - 1, weeksBack); });
  let sampleUserIds = _.map(sample, 'userId');
  sample = _.reject(sample, (u) => { return _.includes(sampleUserIds, u.sponsor && u.sponsor.userId); })
  log.info(`sample=${_.size(sample)}`);

  let nextGen = _.flatten(_.map(sample, downlineUsers));
  nextGen = _.filter(nextGen, (u) => { return createdWithinWeek(u, weeksBack - 2, weeksBack); });
  log.info(`nextGen=${_.size(nextGen)}`);
};
// growth(3);

let announceUsers = () => {
  let announceableUsers = _.filter(users, isAnnounceable);
  _.each(announceableUsers, (u) => {
    firebase.database().ref(`/identityAnnouncementQueue/tasks/${u.userId}`).set({userId: u.userId});
  });
  log.info(`announced ${announceableUsers.length} users`);
}
// announceUsers();

let identityAnnouncementTasks;
let loadIdentityAnnouncementTasks = () => {
  firebase.database().ref(`/identityAnnouncementQueue/tasks`).once('value', (snapshot) => {
    identityAnnouncementTasks = snapshot.val() || {};
    _.each(identityAnnouncementTasks, (t,tid) => { t.taskId = tid; });
    log.info('done loading identityAnnouncementTasks');
    log.info('grouped by state', _.groupBy(identityAnnouncementTasks, '_state'));
    log.info('grouped by error', _.groupBy(identityAnnouncementTasks, '_error_details.error'));
  });
}
loadIdentityAnnouncementTasks();

let users;
let loadUsers = () => {
  usersRef().once('value', (snapshot) => {
    users = snapshot.val() || {};
    _.each(users, (u, uid) => { u.userId = uid; });
    log.info(`${_.size(users)} users loaded`);
    log.info('user groups', userGroups());
  });
}
loadUsers();

let Web3 = require('web3');
let web3 = new Web3(new Web3.providers.HttpProvider('https://www.relay.ur.technology:9596'));
