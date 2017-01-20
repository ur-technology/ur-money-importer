import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as log from 'loglevel';
import {BigNumber} from 'bignumber.js';
import {sprintf} from 'sprintf-js';

export class DataManipulator {
  env: any;
  db: any;
  auth: any;
  users: any;
  stop: boolean;

  constructor(env: any, db: any, auth: any) {
    this.env = env;
    this.db = db;
    this.auth = auth;
    // _.uniqBy = require('lodash.uniqby');
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
    return referrals.concat(_.flatten(_.map(referrals, this.downlineUsers)));
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

  private simplifyUser(u: any) {
    let simplifiedUser = _.pick(u, ['firstName', 'lastName', 'email', 'phone', 'sponsor', 'wallet']);
  }

  loadUsers(lastAddress?: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      if (!lastAddress) {
        self.users = {};
        lastAddress = '0x';
      }
      let batchSize = 5000;

      log.info(`retrieving users with address ${lastAddress + '-'} through 9x...`);
      self.ref("/users").orderByChild('wallet/address').limitToFirst(batchSize).startAt(lastAddress + '-').endAt("9x").once("value", (snapshot: firebase.database.DataSnapshot) => {
        let newUsers = _.mapValues(snapshot.val() || {}, (user, userId) => {
          return _.merge(_.pick(user, ['name', 'email', 'phone', 'wallet', 'sponsor', 'disabled', 'registration']), {userId: userId});
        });

        let numNewUsers = _.size(newUsers);
        log.info(`  ...retrieved ${numNewUsers} users`);

        if (numNewUsers > 0) {
          let newUsersSortedByAddress = _.sortBy(newUsers, 'wallet.address');
          let firstAddress: string = (<any>_.first(newUsersSortedByAddress)).wallet.address;
          lastAddress = (<any>_.last(newUsersSortedByAddress)).wallet.address;
          log.info(`  ...first address: ${firstAddress}`);
          log.info(`  ...last address: ${lastAddress}`);
        }

        _.extend(self.users, newUsers);

        if (numNewUsers < batchSize) {

          log.info(`total users retrieved: ${_.size(self.users)}`);

          let disabledUsers = _.filter(self.users, 'disabled');
          let nonDisabledUsers = _.reject(self.users, 'disabled');
          log.info(`1.1 disabled users: ${_.size(disabledUsers)}`);
          log.info(`1.2 nonDisabled users: ${_.size(nonDisabledUsers)}`);

          let usersWithBonuses = _.filter(nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
          let usersWithoutBonuses = _.reject(nonDisabledUsers, 'wallet.announcementTransaction.blockNumber');
          log.info(`1.2.1 usersWithBonuses: ${_.size(usersWithBonuses)}`);
          log.info(`1.2.2 usersWithoutBonuses: ${_.size(usersWithoutBonuses)}`);

          let blockedUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return self.isBlocked(u); });
          let announcementBeingProcessedUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.announcementBeingProcessed(u); });
          let readyForAnnouncementUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.readyForAnnouncement(u); });
          let otherUsers: any = _.filter(usersWithoutBonuses, (u: any) => { return !self.isBlocked(u) && self.readyForAnnouncement(u); });
          log.info(`1.2.2.1 blockedUsers: ${_.size(blockedUsers)}`);
          log.info(`1.2.2.2 announcementBeingProcessedUsers: ${_.size(announcementBeingProcessedUsers)}`);
          log.info(`1.2.2.3 readyForAnnouncementUsers: ${_.size(readyForAnnouncementUsers)}`);

          // global.gc();
          resolve();

        } else {
          self.loadUsers(lastAddress).then(() => {
            resolve();
          })
        }
      });
    });
  }
}
