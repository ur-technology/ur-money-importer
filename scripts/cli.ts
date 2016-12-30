// step 1

/// <reference path="../typings/index.d.ts" />

import * as firebase from 'firebase';
import * as _ from 'lodash';

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
  return firebase.database().ref(`/users`);
};

let userRef = (id) => {
  return usersRef().child(id);
};


let getUserStatus;
getUserStatus = (user) => {
  let status = _.trim((user.registration && user.registration.status) || '') || 'initial';
  if (status === 'initial' && user.wallet && user.wallet.address) {
    status = 'wallet-generated';
  }
  return status;
}

let userGroups;
userGroups = () => {
  return _.groupBy(users, (u) => {
   return getUserStatus(u) + ` - sponsor ${ u.sponsor && u.sponsor.announcementTransactionConfirmed ? '' : 'not' } confirmed`;
 });
}

let users;
let initializeMetaData;
let addDownlineInfoToUpline;
let roundNumbers;
let potentialUrRewards;
let numDirectReferrals;
let numDownlineUsers;
let bigPotentialRewards;
let bigDirectReferrals;
let bigDownlineUsers;

let hasWallet;
let fertile;
let announcementAlreadyInitiated;
let scenario0;
let scenario0Users;
let scenario1;
let scenario1Users;
let scenario2;
let scenario2Users;
let scenario3;
let scenario3Users;
let scenario4;
let scenario4Users;
let scenario5;
let scenario5Users;
let scenario6;
let scenario6Users;
let description;

let csv;
let json2csv;
let fs;

potentialUrRewards = (u) => { return u.d.potentialUrRewards; };
numDirectReferrals = (u) => { return u.d.numDirectReferrals; };
numDownlineUsers = (u) => { return u.d.numDownlineUsers; };

initializeMetaData = (u: any) => {
  let firstName: string = _.trim(u.firstName || '');
  if (/[^a-zA-Z ]/.test(firstName)) {
    firstName = '';
  }
  u.d = {
    firstName: firstName || 'UR Member',
    potentialUrRewards: 0,
    numDirectReferrals: 0,
    numDownlineUsers: 0
  };
};

roundNumbers = (u: any) => {
  u.d.potentialUrRewards = Math.round(u.d.potentialUrRewards);
};

addDownlineInfoToUpline = (u) => {
  let status = (u.registration && u.registration.status) || 'initial';
  if (u.disabled && status === 'announcement-confirmed') {
    return;
  }
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
    targetUser.d.potentialUrRewards = (targetUser.d.potentialUrRewards || 0) + bonuses[level];
  }
}

hasWallet = (u: any): boolean => {
  return !!u.wallet && !!u.wallet.address;
}

fertile = (u: any): boolean => {
  if (_.isUndefined(u.d.fertile)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.fertile = !u.disabled && hasWallet(u) && (!sponsor || fertile(sponsor));
  }
  return u.d.fertile;
}

announcementAlreadyInitiated = (u: any): boolean => {
  let status = (u.registration && u.registration.status) || 'initial';
  let initiatedStatuses = [ 'announcement-requested', 'announcement-initiated', 'announcement-confirmed' ];
  return _.includes(initiatedStatuses, status);
};

scenario0 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario0)) {
    u.d.scenario0 = announcementAlreadyInitiated(u);
  }
  return u.d.scenario0;
}

scenario1 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario1)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.scenario1 = !u.disabled && hasWallet(u) && !!sponsor && fertile(sponsor) && !announcementAlreadyInitiated(u);
  }
  return u.d.scenario1;
}

scenario2 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario2)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.scenario2 = !u.disabled && hasWallet(u) && !!sponsor && !fertile(sponsor);
  }
  return u.d.scenario2;
}

scenario3 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario3)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.scenario3 = !u.disabled && !hasWallet(u) && !!sponsor && !fertile(sponsor) && u.d.numDownlineUsers === 0;
  }
  return u.d.scenario3;
}

scenario4 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario4)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.scenario4 = !u.disabled && !hasWallet(u) && u.d.numDownlineUsers > 0
  }
  return u.d.scenario4;
}

scenario5 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario5)) {
    let sponsor:any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    u.d.scenario5 = !u.disabled && !hasWallet(u) && !!sponsor && fertile(sponsor) && u.d.numDownlineUsers === 0;
  }
  return u.d.scenario5;
}

scenario6 = (u: any): boolean => {
  if (_.isUndefined(u.d.scenario6)) {
    u.d.scenario6 = !!u.disabled;
  }
  return u.d.scenario6;
}


usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  _.each(users, (u, uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);
  log.info('user groups', userGroups());
});

// step 2

_.each(users, initializeMetaData);
_.each(users, addDownlineInfoToUpline);
_.each(users, roundNumbers);
bigPotentialRewards = _.takeRight(_.sortBy(users, potentialUrRewards),20);
bigDirectReferrals = _.takeRight(_.sortBy(users, numDirectReferrals),20);
bigDownlineUsers = _.takeRight(_.sortBy(users, numDownlineUsers),20);

scenario0Users = _.filter(users, scenario0);
scenario1Users = _.filter(users, scenario1);
scenario2Users = _.filter(users, scenario2);
scenario3Users = _.filter(users, scenario3);
scenario4Users = _.filter(users, scenario4);
scenario5Users = _.filter(users, scenario5);
scenario6Users = _.filter(users, scenario6);

json2csv = require('json2csv');
fs = require('fs');

_.each([
  {users: scenario1Users, jobName: 'scenario1'},
  {users: scenario2Users, jobName: 'scenario2'},
  {users: scenario3Users, jobName: 'scenario3'},
  {users: scenario4Users, jobName: 'scenario4'},
  {users: scenario5Users, jobName: 'scenario5'}
], (job) => {
  csv = json2csv({
    data: _.filter(job.users, 'email'),
    fields: ['email', 'd.firstName', 'd.numDirectReferrals', 'd.numDownlineUsers', 'd.potentialUrRewards'],
    fieldNames: ['email', 'firstName', 'numDirectReferrals', 'numDownlineUsers', 'potentialUrRewards']
  });

  fs.writeFile(`${job.jobName}.csv`, csv, (err) => {
    if (err) throw err;
    console.log('file saved');
  });
});

description = (u) => {
  return (u.d.scenario0 ? '0' : '') + (u.d.scenario1 ? '1' : '') + (u.d.scenario2 ? '2' : '') +
    (u.d.scenario3 ? '3' : '') + (u.d.scenario4 ? '4' : '') + (u.d.scenario5 ? '5' : '') +
    (u.d.scenario6 ? '6' : '');
};
_.groupBy(_.values(users), description);


let announceUsers;
announceUsers = () => {
  let initiatedCount = 0;
  _.each(scenario1Users, (u, index) => {
    let sponsor: any = u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
    if (!u.disabled &&
      !announcementAlreadyInitiated(u) &&
      u.wallet &&
      u.wallet.address &&
      sponsor &&
      !sponsor.disabled &&
      sponsor.wallet &&
      sponsor.wallet.address &&
      sponsor.wallet.announcementTransaction &&
      sponsor.wallet.announcementTransaction.blockNumber &&
      sponsor.wallet.announcementTransaction.hash) {
      firebase.database().ref(`/identityAnnouncementQueue/tasks/${u.userId}`).set({userId: u.userId});
      u.d.initiatedAnnouncement = true;
      initiatedCount++;
    } else {
      u.d.initiatedAnnouncement = false;
    }
  });
  log.info(`initiatedCount: `, initiatedCount);
}
announceUsers();
