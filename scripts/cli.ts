// step 1

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
  return firebase.database().ref(`/users`);
};

let userRef = (id) => {
  return usersRef().child(id);
};


let getUserStatus;
getUserStatus = (user) => {
  let status = _.trim((user.registration && user.registration.status) || '');
  if (status !== 'announcement-confirmed' && status != 'announcement-initiated')  {
    if (user.wallet && user.wallet.address) {
     let sponsor = users[user.sponsor && user.sponsor.userId];
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

let Web3 = require('web3');
let web3 = new Web3(new Web3.providers.HttpProvider('https://www.relay.ur.technology:9596'));

usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  _.each(users, (u, uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);
  log.info('user groups', userGroups());
});


let orphans = _.filter(users, (u) => { return u.sponsor && u.sponsor.userId === unknown.userId; });

let fixOrphans = () => {
  _.each(orphans, (o) => {
    // let sponsorName = o.prefineryUser && o.prefineryUser.userReportedReferrer.toLowerCase();
    // let sponsors = _.filter(users, (x) => { return x.name.toLowerCase() === sponsorName; });
    //
    // if (_.size(sponsors) !== 1 || sponsors[0].userId === o.userId) {
    //   return undefined;
    // }
    let sponsor = eiland;
    let sponsorInfo = _.pick(sponsor, ['name', 'profilePhotoUrl', 'userId']);
    sponsorInfo.announcementTransactionConfirmed = !!sponsor.wallet &&
      !!sponsor.wallet.announcementTransaction &&
      !!sponsor.wallet.announcementTransaction.blockNumber &&
      !!sponsor.wallet.announcementTransaction.hash;
    userRef(o.userId).update({sponsor: sponsorInfo});
  });
};
fixOrphans();



let orphans = _.filter(users, (u) => { return u.sponsor && u.sponsor.userId === unknown.userId; });
let fixOrphans = () => {
  _.each(orphans, (o) => {
    let reportedSponsorName = o.prefineryUser && o.prefineryUser.userReportedReferrer;
    let reportedSponsorLastName = _.last(reportedSponsorName.split(" "));
    let sponsors = _.filter(users, (x) => {
      return x.name && x.name.toLowerCase() === reportedSponsorName.toLowerCase() && x.userId !== o.userId;
    });
    if (_.size(sponsors) === 1) {
      log.info(`${o.email} - ${o.name} sponsored by ${ sponsors[0].name} ?`);
    }
  });
};
fixOrphans();


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
bigBlockers();

let blocker = (u) => {
  let sponsor = users[u.sponsor && u.sponsor.userId];
  if (!u.wallet || !u.wallet.address || u.disabled || !sponsor) {
    return u;
  }
  if (sponsor.email === 'eiland@ur.technology') {
    return undefined;
  }
  return blocker(sponsor);
};

let showFDBlockers = () => {
  let emails = [
    "jojovallejo@gmail.com",
    "doerbank80@gmail.com",
    "nnamanichima@gmail.com",
    "xtianbitcoin@gmail.com",
    "fajardoperez1965@gmail.com",
    "lawalbasit89@gmail.com",
    "jazzua@yandex.ru",
    "safiullahkhankhalil@gmail.com",
    "jr1802@yahoo.com.au",
    "eberenc@gmail.com",
    "nguyendinhdung141294@gmail.com",
    "roberttrantnmu@gmail.com",
    "baligning@gmail.com",
    "prabhatsinha281@gmail.com"
  ];
  return _.map(emails, (email) => {
    let u = _.find(users, {email: email});
    let b = blocker(u);
    return `${email} is blocked by ${b ? (b === u ? 'self' : (b.email || b.name)) : 'no one'}`;
  }).join("\n");
}
showFDBlockers();




let userValues = _.values(users);
let stop = false;
let refs = [];
let batchSize = 500;
let deleteDuplicateEvents = (startingUserIndex) => {
  if (stop || startingUserIndex >= _.size(userValues)) { log.info('exiting'); return; }
  let slice = _.slice(userValues,startingUserIndex,startingUserIndex+batchSize);
  _.each(slice, (u) => {
    if (stop) { log.info('exiting'); return; }
    let events = u.events || {};
    _.each(events, (e,eid) => { e.eventId = eid; });
    events = _.filter(events, { sourceType: 'transaction' });
    let g = _.groupBy(events, 'sourceId');
    _.each(g, (eventGroup, transactionId) => {
      if (stop) { log.info('exiting'); return; }
      if (eventGroup.length > 1) {
        _.each(_.slice(eventGroup, 1), (e) => {
          if (stop) { log.info('exiting'); return; }
          refs.push(userRef(u.userId).child(`events/${e.eventId}`).toString());
        });
      };
    });
  });
  log.info("waiting 3 second...")
  _.delay(deleteDuplicateEvents, 500, startingUserIndex + batchSize);
}
deleteDuplicateEvents(0);



let transactionIds = _.uniq(_.flatten(_.map(users, (u) => {
  return _.keys(u.transactions || {});
})));

let stop = false;
let badTransactionIds = [];
let batchSize = 50;
let findBadTransactionIds = (startingIndex) => {
  let slice = _.slice(transactionIds,startingIndex,startingIndex+batchSize);
  if (stop || _.size(slice) === 0) { log.info('exiting'); return; }
  let newBadTransactionIds = _.reject(slice, (tid) => {
    return web3.eth.getTransaction(tid);
  });
  badTransactionIds = badTransactionIds.concat(newBadTransactionIds);
  log.info(`found ${_.size(newBadTransactionIds)} bad transaction ids`);
  log.info("waiting 2 seconds...")
  _.delay(findBadTransactionIds, 2000, startingIndex + batchSize);
}
findBadTransactionIds(3200);
_.size(badTransactionIds);

let str = `send email, sender user id,receiver address, ur amount\n`;
_.each(users, (u) => {
  let badTransactionsForThisUser = _.pick(u.transactions || {}, badTransactionIds);
  _.each(badTransactionsForThisUser, (tx, txid) => {
    let urAmount = parseInt(tx.amount,16) / 1000000000000000000;
    str = str + `${u.email}, ${u.userId}, ${tx.urTransaction.to}, ${urAmount}\n`
    let ref = userRef(u.userId).child(`transactions/${txid}`);
    ref.remove();
  });
});
log.info(str);

let stop = false;
let userValues = _.filter(users, 'wallet.announcementTransaction.blockNumber');
userValues = _.filter(userValues, 'wallet.announcementTransaction.hash');
let badWalletUsers = [];
let batchSize = 25;
let findBadWalletUsers;
findBadWalletUsers = (startingIndex) => {
  let slice = _.slice(userValues,startingIndex,startingIndex+batchSize);
  if (stop || _.size(slice) === 0) { log.info('exiting'); return; }
  let newBadWalletUsers = _.filter(slice, (u) => {
    if (stop) { return false; }
    let userTransaction = (u.transactions || {})[u.wallet.announcementTransaction.hash];
    if (!userTransaction || !userTransaction.urTransaction || userTransaction.urTransaction.blockNumber !== u.wallet.announcementTransaction.blockNumber) {
      log.info(`no matching transaction for user ${u.userId}`);
      return true;
    }
    // let b = web3.eth.getBalance(u.wallet.address).toNumber();;
    // if (!b) {
    //   log.info(`got zero balance for user ${u.userId}`);
    //   return true;
    // }
    return false;
  });
  badWalletUsers = badWalletUsers.concat(newBadWalletUsers);
  log.info(`found ${_.size(newBadWalletUsers)} in wallets ${startingIndex} to ${startingIndex + batchSize - 1}...`);
  _.delay(findBadWalletUsers, 2000, startingIndex + batchSize);
}
findBadWalletUsers(625);
_.size(badWalletUsers);

_.size(userValues)

log.info(`deleteCount=${_.size(refs)}`);
stop=true

let now;
now = new Date().valueOf();

let userEvents = [];
let userTransactions = [];
_.each(users, (u) => {
  let transactions = u.transactions || {};
  let events = u.events || {};
  _.each(events, (e, eventId) => {
    if (e.sourceType === 'transaction') {
      let t = transactions[e.sourceId];
      if (!t) {
        throw `could not find transaction ${e.sourceId} from event ${eventId} for user ${u.userId}`;
      }
      if (t.urTransaction && t.urTransaction.blockNumber >= 444000) {
        userEvents.push({userId: u.userId, eventId: eventId});
      }
    }
  });
  _.each(transactions, (t, transactionId) => {
    if (t.urTransaction && t.urTransaction.blockNumber >= 444000) {
      userTransactions.push({userId: u.userId, transactionId: transactionId});
    }
  });
});
log.info(`_.size(userEvents)=${_.size(userEvents)}`);
log.info(`_.size(userTransactions)=${_.size(userTransactions)}`);

_.each(userEvents, (userEvent) => {
  let ref = firebase.database().ref(`/users/${userEvent.userId}/events/${userEvent.eventId}`);
  ref.remove();
});
_.each(userTransactions, (userTransaction) => {
  let ref = firebase.database().ref(`/users/${userTransaction.userId}/transactions/${userTransaction.transactionId}`);
  ref.remove();
});


let updateCurrentBalance;
let Web3;
let web3;
let count;
let needsBalance;
let usersNeedingBalance;



count = 0;
updateCurrentBalance = (u: any) => {
  if (!u.userId) {
    throw 'no userId found';
  }

  if (!u.wallet || !u.wallet.address) {
    throw `no user address found`;
  }

  if (u.wallet.currentBalance) {
    return;
  }

  let currentBalance: BigNumber;
  try {
    currentBalance = web3.eth.getBalance(u.wallet.address);
  } catch(error) {
    throw `got error when attempting to get balance for address ${u.wallet.address} and user ${u.userId}`;
  }

  userRef(u.userId).child('wallet').update({currentBalance: currentBalance.toFixed()}).then(() => {
    u.wallet.currentBalance = currentBalance.toFixed();
    // log.info(`updated user ${u.userId} to ${currentBalance.toFormat(0)}`);
    log.info(` updated balance for user ${count}`);
    count++;

  }, (error: any) => {
    thow `could not update current balance for user ${u.userId}: ${error}`;
  });

}

// let john = _.find(users, {email: 'jreitano@ur.technology'});
// updateCurrentBalance(john);

let t1, t2, batchSize;
let remainingItems;

needsBalance = (u) => { return !!u.wallet && !!u.wallet.announcementTransaction && !!u.wallet.announcementTransaction.blockNumber && !u.wallet.currentBalance; };


usersNeedingBalance = _.filter(users, needsBalance);
log.info(`${_.size(usersNeedingBalance)} still need balance`);
batchSize = 700;
_.each(_.slice(usersNeedingBalance,0,batchSize), updateCurrentBalance);




let Web3 = require('web3');
let web3 = new Web3(new Web3.providers.HttpProvider('https://relay.ur.technology:9596'));
let filter = web3.eth.filter({fromBlock: 0, toBlock: 'latest', address: '0x482cf297b08d4523c97ec3a54e80d2d07acd76fa'});
let filterResults = filter.get((error, logEntries) => {
  _.each(logEntries, (logEntry) => {
    log.info(logEntry);
  });
});

filterResults = filter.get(function(error, logEntries) {
  logEntries.forEach(function(logEntry) {
    log.info(logEntry);
  });
});

log.info(filterResults);







function getTransactionsByAccount(myaccount, startBlockNumber, endBlockNumber) {
  if (endBlockNumber == null) {
    endBlockNumber = eth.blockNumber;
  }
  if (startBlockNumber == null) {
    startBlockNumber = endBlockNumber - 1000;
  }

  var transactions = [];

  for (var i = startBlockNumber; i <= endBlockNumber; i++) {
    if (i % 1000000 == 1) {
      console.log("Searching block " + i);
    }
    var block = eth.getBlock(i, true);
    if (block != null && block.transactions != null) {
      block.transactions.forEach( function(e) {
        if (myaccount == "*" || myaccount == e.from || myaccount == e.to) {
          transactions.push(e);
        }
      })
    }
  }

  console.log('loaded', transactions.length, 'transactions');
}

let growth;
growth = (a, b) => {
  let now = new Date().valueOf();
  let sample = _.filter(users, (u) => {
    let hoursAgoCreated = (now - u.createdAt)/60/60/1000;
    return a < hoursAgoCreated && hoursAgoCreated <= b && u.wallet && u.wallet.announcementTransaction && u.wallet.announcementTransaction.blockNumber;
  });
  log.info(`size=${_.size(sample)}`);
  let nextGen = _.filter(users, (u) => {
    return u.sponsor && _.some(sample, (s) => { return s.userId === u.sponsor.userId; });
  });
  log.info(`sample size=${_.size(sample)}, next gen size=${_.size(nextGen)}, growth rate=${(_.size(nextGen) - _.size(sample))/ _.size(sample) * 100}`);
};
growth(12, 24);



let x, y;
x = _.groupBy(users, 'phone');

y = _.filter(x, (group, phone) => {
  if (!phone || group.length === 1 || group.length > 2000) {
    return false;
  }
  let haveWallets = _.filter(group, 'wallet.announcementTransaction');
  return haveWallets.length > 1;
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
  _.each(users, (u, index) => {
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


let needsAnnouncement;
needsAnnouncement = (u) => {
  let sponsor: any = u && u.sponsor && u.sponsor.userId && users[u.sponsor && u.sponsor.userId];
  return !!u &&
    !u.disabled &&
    !announcementAlreadyInitiated(u) &&
    !!u.wallet &&
    !!u.wallet.address &&
    !!sponsor &&
    !sponsor.disabled &&
    !!sponsor.wallet &&
    !!sponsor.wallet.address &&
    !!sponsor.wallet.announcementTransaction &&
    !!sponsor.wallet.announcementTransaction.blockNumber &&
    !!sponsor.wallet.announcementTransaction.hash;
}

let usersNeedingAnnouncement;
usersNeedingAnnouncement = _.filter(users, needsAnnouncement);

_.each(usersNeedingAnnouncement, (u) => {
  firebase.database().ref(`/identityAnnouncementQueue/tasks/${u.userId}`).set({userId: u.userId});
});


let growth;
growth = (now, a, b) => {
  let sample = _.filter(users, (u) => {
    let timeAgo = now - u.createdAt; let period = 60 * 60 * 1000;
    return (a * period) < timeAgo && timeAgo  <= (b * period) && u.wallet && u.wallet.announcementTransaction;
  });
  let nextGen = _.filter(users, (u) => {
    return u.sponsor &&
      _.some(sample, (s) => { return s.userId === u.sponsor.userId; });
  });
  log.info(`sample size=${_.size(sample)}, next gen size=${_.size(nextGen)}, growth rate=${(_.size(nextGen) - _.size(sample))/ _.size(sample) * 100}`);
};
growth(now, 24, 36);




let tasks;
firebase.database().ref('/phoneAuthQueue/tasks').once('value').then((snapshot) => {
  tasks = snapshot.val() || {};
  _.each(tasks, (t, tid) => { t.taskId = tid; });
});





let getTransactionsByAccount = (startBlockNumber, endBlockNumber) => {
  var count = 0;
  for (var i = startBlockNumber; i <= endBlockNumber; i++) {
    var block = web3.eth.getBlock(i, true);
    if (block != null && block.transactions != null) {
      block.transactions.forEach( function(e) {
        if (e.input && e.input.length === 4) {
          count++;
        }
      });
    }
  }
  console.log("Found " + count + " signup transactions");
}
getTransactionsByAccount(444000,445000);



let idTasks;
let a, b, c, d;
firebase.database().ref(`/identityAnnouncementQueue/tasks`).once('value', (snapshot) => {
  idTasks = snapshot.val() || {};
  _.each(idTasks, (t,tid) => { t.taskId = tid; });
  log.info('done loading tasks');
  log.info('grouped by state', _.groupBy(idTasks, '_state'));
  log.info('grouped by error', _.groupBy(idTasks, '_error_details.error'));

  a = _.filter(idTasks, (t) => {
    return t._state === 'error' && t._error_details && /announcement-initiated/.test(t._error_details.error);
  });

});

let now = new Date().valueOf();
let readyToGo = _.filter(users, (u) => {
  let sponsor = users[u.sponsor && u.sponsor.userId];
  if (u.disabled || !u.wallet || !u.wallet.address) {
    return false;
  }
  if ((now - u.wallet.createdAt)/60/1000 <= 2) {
    return false
  }
  let status = getUserStatus()
  return !u.wallet.announcementTransaction &&

    sponsor && !sponsor.diabled &&
    sponsor.wallet && sponsor.wallet.announcementTransaction && sponsor.wallet.announcementTransaction.blockNumber;
});
_.groupBy(readyToGo, 'registration.status');

_.each(readyToGo, (u) => {
  firebase.database().ref(`/users/${u.userId}/registration/status`).remove();
  firebase.database().ref(`/identityAnnouncementQueue/tasks`).push({userId: u.userId});
});

_.each(c, (t) => {
  let u = users[t.userId];
  let ref = userRef(u.userId);
  let ref1 = ref.child('wallet').child('announcementTransaction');
  // log.info(`ref1=${ref1.toString()}`);
  ref1.remove();
  let ref2 = ref.child('registration').child('status');
  // log.info(`ref2=${ref2.toString()}`);
  ref2.remove();
  let ref3 = firebase.database().ref(`/identityAnnouncementQueue/tasks/${t.taskId}`);
  // log.info(`ref3=${ref3.toString()}`);
  ref3.remove();
});

stop = true

let stop = false;
let showBlocks = (startingBlock) => {
  for (let i = 0; i < 50; i++) {
    let b = startingBlock + i;
    let numTrans = (web3.eth.getBlock(b, true).transactions || []).length;
    if (numTrans) {
      log.info(`block ${b} has ${numTrans} transactions`);
    }
  }
  if (!stop) {
    _.delay(showBlocks, 250, startingBlock + 50);
  }
}
showBlocks(493300);

_.map(Array.from(Array(100).keys()), (i) => {
  return (web3.eth.getBlock(b, true).transactions || []).length;
})


_.each(a, (t) => {
  let u = users[t.userId];
  let ref3 = firebase.database().ref(`/identityAnnouncementQueue/tasks/${t.taskId}`);
  ref3.remove();
});


_.filter(users, (u) => { return u.wallet && u.wallet.announcementTransaction && u.wallet.announcementTransaction.blockNumber; })
