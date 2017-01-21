/// <reference path="../typings/index.d.ts" />

import * as dotenv from 'dotenv';
import * as _ from 'lodash';

let BigNumber = require('bignumber.js');

let log = {
  info: console.log,
  debug: console.log,
  trace: console.log,
  setDefaultLevel: (x) => {}
}; // couldn't get loglevel to work in Mancy, so faking it this way

class DataManipulator {
  env: any;
  db: any;
  auth: any;
  users: any;
  usersWithBonuses: any[];
  stop: boolean;

  constructor(env: any, db: any, auth: any) {
    this.env = env;
    this.db = db;
    this.auth = auth;
    _.uniqBy = require('lodash.uniqby');
    // _.keyBy = require('lodash.keyby');
    // _.intersectionBy = require('lodash.intersectionby');
  }

  private ref(path: string) {
    return this.db.ref(path);
  }

  private userRef(u: any) {
    return this.ref('/users').child(u.userId);
  }

  private getSponsor(u: any) {
    return this.users[u.sponsor && u.sponsor.userId];
  }

  private hasSponsor(u: any) {
    return !!u.sponsor && !!u.sponsor.userId;
  }

  private directReferrals(u: any) {
    return _.filter(this.users, (x: any) => { return x.sponsor && x.sponsor.userId === u.userId; });
  }

  private getUserStatus(user: any) {
    let status = _.trim((user.registration && user.registration.status) || '');
    if (status !== 'announcement-confirmed' && status !== 'announcement-initiated') {
      if (user.wallet && user.wallet.address) {
        let sponsor = this.getSponsor(user);
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

  private userGroups(sampleUsers: any) {
    sampleUsers = sampleUsers || this.users;
    return _.groupBy(sampleUsers, (u: any) => {
      let sponsorConfirmed = !u.sponsor || (!!this.users[u.sponsor.userId] && !!this.users[u.sponsor.userId].wallet && !!this.users[u.sponsor.userId].wallet.announcementTransaction && !!this.users[u.sponsor.userId].wallet.announcementTransaction.blockNumber);
      return this.getUserStatus(u) + ` - sponsor ${sponsorConfirmed ? '' : 'not'} confirmed`;
    });
  }

  private addDownlineInfoToUpline(u: any) {
    let status = (u.registration && u.registration.status) || 'initial';
    let bonuses = [60.60, 60.60, 121.21, 181.81, 303.03, 484.84, 787.91];

    let targetUser = u;
    for (let level = 0; level < 7; level++) {
      targetUser = targetUser.sponsor && targetUser.sponsor.userId && this.users[targetUser.sponsor.userId];
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

  private hasWallet(u: any): boolean {
    return !!u.wallet && !!u.wallet.address;
  }

  private setSponsorToEiland(u: any) {
    let eiland: any = _.find(this.users, { email: 'eiland@ur.technology' });
    let sponsorInfo: any = _.pick(eiland, ['name', 'profilePhotoUrl', 'userId']);
    sponsorInfo.announcementTransactionConfirmed = true;
    this.userRef(u).update({ sponsor: sponsorInfo });
  }

  private blocker(u: any): any {
    if (u.email === 'eiland@ur.technology' || u.email === 'jreitano@ur.technology') {
      log.info(`${u.email}: hit the top!`);
      return undefined;
    }
    let sponsor: any = this.getSponsor(u);
    if (!sponsor) {
      log.info(`${u.email}: has no sponsor`);
      return undefined;
    }
    if (sponsor.wallet && sponsor.wallet.address && !sponsor.diabled) {
      log.info(`${u.email}: is the sponsor ${sponsor.email} blocked?`);
      return this.blocker(sponsor);
    } else {
      log.info(`${u.email}: the sponsor ${sponsor.email} is a blocker`);
      return sponsor;
    }
  }

  private isTopUser(u: any): boolean {
    return u.email === 'jreitano@ur.technology';
  }

  private walletEnabled(u: any): boolean {
    return this.hasWallet(u) && !u.disabled && (this.hasSponsor(u) || this.isTopUser(u));
  }

  private announcementConfirmed(u: any): boolean {
    return !!u.wallet && !!u.wallet.announcementTransaction && !!u.wallet.announcementTransaction.hash && !!u.wallet.announcementTransaction.blockNumber;
  }

  private announcementBeingProcessed(u: any): boolean {
    return !!u.wallet && !!u.wallet.announcementTransaction && !!u.wallet.announcementTransaction.hash && !u.wallet.announcementTransaction.blockNumber
  }

  private readyForAnnouncement(u: any): boolean {
    return this.walletEnabled(u) && !u.wallet.announcementTransaction;
  }

  private isBlocked(u: any): boolean {
    if (!this.readyForAnnouncement(u)) {
      return false;
    }

    if (u.email === 'eiland@ur.technology' || u.email === 'jreitano@ur.technology') {
      return false;
    }

    let sponsor: any = this.getSponsor(u);
    if (!sponsor || !sponsor.wallet || !sponsor.wallet.address || sponsor.disabled) {
      return true;
    }

    return this.isBlocked(sponsor);
  }

  private newSponsor(u: any): any {
    if (!this.isBlocked(u)) {
      return undefined;
    }

    let currentUser: any = u;
    while (true) {
      currentUser = this.getSponsor(currentUser);
      if (this.walletEnabled(currentUser) && !this.isBlocked(currentUser)) {
        return currentUser;
      }
    }
  }

  private blockedUsers(): any[] {
    return _.filter(this.users, this.isBlocked);
  }

  private isAnnounceable(u: any): boolean {
    return this.readyForAnnouncement(u) && !this.blocker(u);
  }


  private referralUsers(u: any): any[] {
    return _.filter(this.users, (r: any) => {
      return r.sponsor && r.sponsor.userId === u.userId && r.userId !== u.userId
    });
  }

  private downlineUsers(u: any): any[] {
    let referrals: any[] = this.referralUsers(u);
    return referrals.concat(_.flatten(_.map(referrals, (r) => { return this.downlineUsers(r); })));
  }

  private convertPushIdToTimestamp(id: string): number {
    let PUSH_CHARS = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    id = id.substring(0, 8);
    let timestamp = 0;
    for (let i = 0; i < id.length; i++) {
      var c = id.charAt(i);
      timestamp = timestamp * 64 + PUSH_CHARS.indexOf(c);
    }
    return timestamp;
  }

  private clearOldPhoneAuthTasks(): Promise<any[]> {
    let self = this;
    return new Promise((resolve, reject) => {
      let tasksRef = firebase.database().ref(`/phoneAuthQueue/tasks`);
      tasksRef.once('value', (snapshot) => {
        let phoneAuthTasks: any = snapshot.val() || {}
        _.each(phoneAuthTasks, (t, tid) => { t.taskId = tid; });
        log.info('done loading phoneAuthTasks');
        log.info('grouped by state', _.groupBy(phoneAuthTasks, '_state'));
        log.info('grouped by error', _.groupBy(phoneAuthTasks, '_error_details.error'));

        let count = 0;
        let now = new Date().valueOf();
        _.each(phoneAuthTasks, (t) => {
          let createdAt = this.convertPushIdToTimestamp(t.taskId);
          if ((now - createdAt) / 1000 / 60 / 60 > 0.5) {
            tasksRef.child(t.taskId).remove();
            count++;
          }
        });
        log.info(`removed ${count} phoneAuthTasks`);
        resolve(phoneAuthTasks);
      });
    });
  }

  private createdWithinWeeks(u: any, w1: number, w2: number): boolean {
    let now = new Date().valueOf();
    let weeksAgoCreated = (now - u.createdAt) / 1000 / 60 / 60 / 24 / 7;
    return w1 <= weeksAgoCreated && weeksAgoCreated < w2 && u.wallet && u.wallet.address;
  }

  private growth(weeksBack: number) {
    let sample = _.filter(this.users, (u: any) => { return this.createdWithinWeeks(u, weeksBack - 1, weeksBack); });
    let sampleUserIds = _.map(sample, 'userId');
    sample = _.reject(sample, (u: any) => { return _.includes(sampleUserIds, u.sponsor && u.sponsor.userId); })
    log.info(`sample=${_.size(sample)}`);

    let nextGen = _.flatten(_.map(sample, this.downlineUsers));
    nextGen = _.filter(nextGen, (u: any) => { return this.createdWithinWeeks(u, weeksBack - 2, weeksBack); });
    log.info(`nextGen=${_.size(nextGen)}`);
  }

  private announceUsers() {
    let announceableUsers = _.filter(this.users, this.isAnnounceable);
    _.each(announceableUsers, (u: any) => {
      firebase.database().ref(`/identityAnnouncementQueue/tasks/${u.userId}`).set({ userId: u.userId });
    });
    log.info(`announced ${announceableUsers.length} users`);
  }

  private loadIdentityAnnouncementTasks() {
    firebase.database().ref(`/identityAnnouncementQueue/tasks`).once('value', (snapshot) => {
      let identityAnnouncementTasks: any = snapshot.val() || {}
      _.each(identityAnnouncementTasks, (t, tid) => { t.taskId = tid; });
      log.info('done loading identityAnnouncementTasks');
      log.info('grouped by state', _.groupBy(identityAnnouncementTasks, '_state'));
      log.info('grouped by error', _.groupBy(identityAnnouncementTasks, '_error_details.error'));
    });
  }

  kickOffUser(user: any) {
    let users = [user];
    users = users.concat(this.downlineUsers(u));
    _.each(users, (u) => {
      this.userRef(u).update({disabled: true});
      this.auth.deleteUser(u.userId)
    });
  }

  loadUsers(lastKey?: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      if (!lastKey) {
        self.users = {};
        lastKey = '';
      }
      let batchSize = 5000;

      log.info(`retrieving users with address ${lastKey + '-'} through zzzzzzz...`);
      self.ref("/users").orderByKey().limitToFirst(batchSize).startAt(lastKey + '-').endAt("zzzzzzz").once("value", (snapshot: firebase.database.DataSnapshot) => {
        let newUsers = _.mapValues(snapshot.val() || {}, (user, userId) => {
          return _.merge(_.pick(user, ['name', 'email', 'phone', 'wallet', 'sponsor', 'disabled', 'registration', 'transactions', 'countryCode']), {userId: userId});
        });

        let numNewUsers = _.size(newUsers);
        log.info(`  ...retrieved ${numNewUsers} users`);

        if (numNewUsers > 0) {
          let newUsersSortedByAddress = _.sortBy(newUsers, 'userId');
          let firstKey: string = (<any>_.first(newUsersSortedByAddress)).userId;
          lastKey = (<any>_.last(newUsersSortedByAddress)).userId;
          log.info(`  ...first address: ${firstKey}`);
          log.info(`  ...last address: ${lastKey}`);
        }

        _.extend(self.users, newUsers);

        if (numNewUsers === batchSize) {
          // need to retrieve more users
          self.loadUsers(lastKey).then(() => {
            resolve();
          });
        } else {
          // all done retrieving users
          log.info(`total users retrieved: ${_.size(self.users)}`);

          // global.gc();
          resolve();
        }
      });
    });
  }
}

dotenv.config(); // if running on local machine, load config vars from .env file, otherwise these come from heroku

log.setDefaultLevel(process.env.LOG_LEVEL || "info")

log.info(`starting with NODE_ENV ${process.env.NODE_ENV} and FIREBASE_PROJECT_ID ${process.env.FIREBASE_PROJECT_ID}`);

let serviceAccount = require(`./serviceAccountCredentials.${process.env.FIREBASE_PROJECT_ID}.json`);
let admin = require("firebase-admin");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: `https://${process.env.FIREBASE_PROJECT_ID}.firebaseio.com`
});

let dataManipulator = new DataManipulator(process.env, admin.database(), admin.auth());
dataManipulator.loadUsers().then(() => {
  log.info("all done loading users!");

  // let disabledUsers = _.filter(self.users, 'disabled');
  // let nonDisabledUsers = _.reject(self.users, 'disabled');
  // log.info(`1.1 disabled users: ${_.size(disabledUsers)}`);
  // log.info(`1.2 nonDisabled users: ${_.size(nonDisabledUsers)}`);
  //
  // self.usersWithBonuses = _.filter(nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
  // let usersWithoutBonuses = _.reject(nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
  // log.info(`1.2.1 usersWithBonuses: ${_.size(self.usersWithBonuses)}`);
  // log.info(`1.2.2 usersWithoutBonuses: ${_.size(usersWithoutBonuses)}`);
  //
  // let blockedUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return self.isBlocked(u); });
  // let announcementBeingProcessedUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.announcementBeingProcessed(u); });
  // let readyForAnnouncementUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.readyForAnnouncement(u); });
  // let otherUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.readyForAnnouncement(u); });
  // log.info(`1.2.2.1 blockedUsers: ${_.size(blockedUsers)}`);
  // log.info(`1.2.2.2 announcementBeingProcessedUsers: ${_.size(announcementBeingProcessedUsers)}`);
  // log.info(`1.2.2.3 readyForAnnouncementUsers: ${_.size(readyForAnnouncementUsers)}`);

  // let usersWithSentTransactions, sentTransactions, reusedToAddresses, suspects;
  //
  // usersWithSentTransactions = _.filter(dataManipulator.usersWithBonuses, (u) => {
  //   return _.some(u.transactions || {}, {type: 'sent'});
  // });
  //
  // sentTransactions = _.flatten(
  //   _.map(usersWithSentTransactions, (u) => {
  //     return _.uniqBy(_.filter(u.transactions, {type: 'sent'}), 'urTransaction.to');
  //   })
  // );
  //
  // reusedToAddresses = _.keys(
  //   _.pick(
  //     _.groupBy(sentTransactions, 'urTransaction.to'),
  //     (dups, to) => { return dups.length > 2; }
  //   )
  // );
  // suspects = _.filter(dataManipulator.usersWithBonuses, (u) => {
  //   let addresses = _.map(_.filter(u.transactions || {}, {type: 'sent'}), 'urTransaction.to');
  //   return !_.isEmpty(_.intersection(addresses, reusedToAddresses));
  // });
  //
  // log.info(`users who sent ur=${_.size(usersWithSentTransactions)}`);
  // log.info(`suspects=${_.size(suspects)}`);

  // let transactionHashes = _.flatten(
  //   _.uniq(_.flatten(
  //     _.map(dataManipulator.users, (u) => { return _.keys(u.transactions || {}); })
  //   ))
  // );
  // let transactionHashChunks = _.chunk(transactionHashes, 50);
  //
  // let Web3 = require('web3');
  // let web3 = new Web3(new Web3.providers.HttpProvider('http://127.0.0.1:9595'));
  //
  // let getInvalidTransactionHashes = (hashes: string[]): Promise<any> => {
  //   let self = this;
  //   return new Promise((resolve, reject) => {
  //     let invalidHashes: string[] = _.reject(hashes, (h) => { return web3.eth.getTransaction(h); });
  //     log.info(`found ${_.size(invalidHashes)} new invalid hashes`);
  //     resolve(invalidHashes);
  //   });
  // };
  //
  // let invalidHashes: string[] = [];
  // _.each(transactionHashChunks, (chunk: string[]) => {
  //   getInvalidTransactionHashes(chunk).then((newInvalidHashes: string[]) => {
  //     invalidHashes = invalidHashes.concat(newInvalidHashes);
  //   });
  // });
  //
  // _.each(dataManipulator.users, (u) => {
  //   _.each(u.transactions || {}, (t, hash) => {
  //     if (_.includes(invalidHashes, hash)) {
  //       t.invalid = true;
  //       u.invalid = true;
  //     }
  //   });
  // });

});
