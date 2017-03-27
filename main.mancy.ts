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

dotenv.config(); // if running on local machine, load config vars from .env file, otherwise these come from heroku

log.setDefaultLevel(process.env.LOG_LEVEL || "info")

log.info(`starting with NODE_ENV ${process.env.NODE_ENV} and FIREBASE_PROJECT_ID ${process.env.FIREBASE_PROJECT_ID}`);

let serviceAccount = require(`./serviceAccountCredentials.${process.env.FIREBASE_PROJECT_ID}.json`);
let admin = require("firebase-admin");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: `https://${process.env.FIREBASE_PROJECT_ID}.firebaseio.com`
});


class DataManipulator {
  env: any;
  db: any;
  auth: any;
  users: any;
  disabledUsers: any[];
  nonDisabledUsers: any[];
  usersWithBonuses: any[];
  usersWithoutBonuses: any[];
  blockedUsers: any[];
  announcementBeingProcessedUsers: any[];
  readyForAnnouncementUsers: any[];
  otherUsers: any[];
  stop: boolean;
  twilioLookupsClient: any;

  constructor(env: any, db: any, auth: any) {
    this.env = env;
    this.db = db;
    this.auth = auth;
    _.uniqBy = require('lodash.uniqby');
    // file smallUsers.json was generated with the following command:
    //   jq '.users | with_entries(del(.value.chats,.value.chatSummaries, .value.transactions, .value.downlineUsers, .value.events, .value.registration.verificationArgs, .value.registration.verificationResult))' 2017-03-25T01-25-03Z_ur-money-production_data.json > smallUsers.json

    this.users = require('./smallUsers.json');
    _.each(this.users, (u, userId) => { u.userId = userId; });
    log.info(`total users retrieved: ${_.size(this.users)}`);
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

  private unblockUsers() {
    // BUT...need to make sure that unblocking one doesn't obviate the need to unblock others
    // sort self.blockedUsers by dowline level

    let reassigned = 0;
    let notReassigned = 0;

    _.each(self.users, (u, uid) => {
      if (_.isBlocked(u)) {
        let newSponsor = _.find(this.uplineUsers(u), (uplineUser, uplineUserId) => {
          return this.walletEnabled(uplineUser);
        });
        if (newSponsor) {
          // save old sponsor

          // set new sponsor
          reassigned++;
        } else {
          notReassigned++;
        }
      }
    });

    log.info(`${reassigned} blocked users reassigned`);
    log.info(`${notReassigned} blocked users not reassigned`);

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

  lookupCarrier(phone: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      if (!self.twilioLookupsClient) {
        let twilio = require('twilio');
        self.twilioLookupsClient = new twilio.LookupsClient(self.env.TWILIO_ACCOUNT_SID, self.env.TWILIO_AUTH_TOKEN);
      }
      self.twilioLookupsClient.phoneNumbers(phone).get({
        type: 'carrier'
      }, function(error: any, number: any) {
        if (error) {
          reject(`error looking up carrier: ${error.message}`);
          return;
        }
        resolve({
          name: (number && number.carrier && number.carrier.name) || "Unknown",
          type: (number && number.carrier && number.carrier.type) || "Unknown"
        });
      });
    });
  }

  disableAndSignOutUser(u: any) {
    this.userRef(u).update({disabled: true});
    this.auth.deleteUser(u.userId)
  }

  uplineUsers(u: any) {
    let upline: any[] = [];
    let currentUser: any = u;
    while(currentUser.sponsor) {
      currentUser = this.getSponsor(currentUser);
      upline.push(currentUser);
    }
    return upline;
  }

  displayStats() {
    let self = this;
    self.disabledUsers = _.filter(self.users, 'disabled');
    self.nonDisabledUsers = _.reject(self.users, 'disabled');
    log.info(`1.1 disabled users: ${_.size(self.disabledUsers)}`);
    log.info(`1.2 nonDisabled users: ${_.size(self.nonDisabledUsers)}`);

    self.usersWithBonuses = _.filter(self.nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
    self.usersWithoutBonuses = _.reject(self.nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
    log.info(`1.2.1 usersWithBonuses: ${_.size(self.usersWithBonuses)}`);
    log.info(`1.2.2 usersWithoutBonuses: ${_.size(self.usersWithoutBonuses)}`);

    self.blockedUsers = _.filter(self.usersWithoutBonuses, (u: any) => { return self.isBlocked(u); });
    log.info(`1.2.2.1 blockedUsers: ${_.size(self.blockedUsers)}`);

    self.announcementBeingProcessedUsers = _.filter(self.usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.announcementBeingProcessed(u); });
    log.info(`1.2.2.2 announcementBeingProcessedUsers: ${_.size(self.announcementBeingProcessedUsers)}`);

    self.readyForAnnouncementUsers = _.filter(self.usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && !self.announcementBeingProcessed(u) && self.readyForAnnouncement(u); });
    log.info(`1.2.2.3 readyForAnnouncementUsers: ${_.size(self.readyForAnnouncementUsers)}`);

    self.otherUsers = _.filter(self.usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && !self.announcementBeingProcessed(u) && !self.readyForAnnouncement(u); });
    log.info(`1.2.2.4 otherUsers: ${_.size(self.otherUsers)}`);
  }

  incorrectDownlineLevels() {
    return _.filter(this.users, (u: any, uid: string) => {
      return u.downlineLevel !== this.uplineUsers(u).length + 1;
    });
  }

  fixDownlineLevels() {
    let self = this;
    let i = 0;
    _.each(self.users, (u: any, uid: string) => {
      let upline: any[] = self.uplineUsers(u);
      if (u.downlineLevel !== upline.length + 1) {
        if (i++ % 1000 === 0) {
          log.info(`i: ${i}`);
        }
        self.userRef(u).update({downlineLevel: upline.length + 1 });
      }
    });
  }
}

let dataManipulator = new DataManipulator(process.env, admin.database(), admin.auth());
dataManipulator.displayStats();

dataManipulator.incorrectDownlineLevels();
