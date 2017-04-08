import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as log from 'loglevel';
import {BigNumber} from 'bignumber.js';
import {sprintf} from 'sprintf-js';
import {ManualIDVerifier} from './manual';

export class DataManipulator {
  env: any;
  db: any;
  auth: any;
  users: any;
  disabledUsers: any[];
  nonDisabledUsers: any[];
  usersWithBonuses: any[];
  usersWithoutWallets: any[];
  usersWithWalletsButNoBonuses: any[];


  usersWithDocsPending: any[];
  usersWithReviewPending: any[];
  approvedUsersWithAnnouncementPending: any[];
  otherUsers: any[];

  twilioLookupsClient: any;

  constructor(env: any, db: any, auth: any) {
    this.env = env;
    this.db = db;
    this.auth = auth;
    // file smallUsers.json was generated with the following command:
    //   jq '.users | with_entries(del(.value.chats,.value.chatSummaries, .value.transactions, .value.events, .value.registration.verificationArgs, .value.registration.verificationResult))' ./data/ur-money-production_data.json > ./data/smallUsers.json

    this.users = require('../data/smallUsers.json');
    _.each(this.users, (u, userId) => {
      u.userId = userId;
      u.state = this.userState(u);
    });

    log.info(`loaded ${_.size(this.users)} users`);
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

  private setSponsorToEiland(u: any) {
    let eiland: any = _.find(this.users, { email: 'eiland@ur.technology' });
    let sponsorInfo: any = _.pick(eiland, ['name', 'profilePhotoUrl', 'userId']);
    sponsorInfo.announcementTransactionConfirmed = true;
    this.userRef(u).update({ sponsor: sponsorInfo });
  }

  private isTopUser(u: any): boolean {
    return u.email === 'jreitano@ur.technology';
  }

  private userState(u: any): string {
    if (!this.isTopUser(u) && !u.sponsor) {
      return 'missing-sponsor';
    } else if (!u.wallet || !u.wallet.address) {
      return 'missing-wallet';
    } if (u.disabled) {
      return 'disabled'
    } if (u.fraudSuspected) {
      return 'fraud-suspected'
    } else if (u.wallet.announcementTransaction && u.wallet.announcementTransaction.hash && u.wallet.announcementTransaction.blockNumber) {
      return 'announcement-confirmed';
    } else if (u.wallet.announcementTransaction && u.wallet.announcementTransaction.hash) {
      return 'waiting-for-announcement-to-be-confirmed';
    } else if (!u.signUpBonusApproved && !(u.idUploaded && u.selfieMatched)) {
      return 'waiting-for-docs';
    } else if (!u.signUpBonusApproved) {
      return 'waiting-for-review';
    } else if (!u.sponsor.announcementTransactionConfirmed) {
      let s = this.sponsorState(u);
      switch(s) {
        case 'missing-sponsor':
        case 'missing-wallet':
        case 'disabled':
        case 'fraud-suspected':
        case 'waiting-for-docs':
        case 'blocked-by-upline':
          return `blocked-by-upline`

        case 'waiting-for-announcement-confirmation':
        case 'waiting-for-review':
        case 'waiting-for-announcement-to-be-queued':
        case 'waiting-for-upline-processing':
          return 'waiting-for-upline-processing';

        default:
          return `unexpected-sponsor-state: ${s}`;
      }
    } else {
      return 'waiting-for-announcement-to-be-queued';
    }
  }

  private sponsorState(u: any): any {
    let sponsor = this.getSponsor(u);
    return sponsor && this.userState(sponsor);
  }

  private rootUser(u: any): any {
    if (_.includes(['blocked-by-upline', 'waiting-for-upline-processing'], this.userState(u))) {
      return this.rootUser(this.getSponsor(u));
    } else {
      return u;
    }
  }

  private rootState(u: any): any {
    return this.userState(this.rootUser(u));
  }

  private blocker(u: any): any {
    if (this.userState(u) !== 'blocked') {
      return undefined;
    }

    let upline = this.uplineUsers(u);
    return _.find(this.uplineUsers(u), (up) => {
      return this.userState(up) !== 'blocked';
    });
  }

  fixDownlineInfo() {
    let self = this;
    let updatedDownlineLevels = 0;
    let updatedDownlineUsers = 0;
    let updatedDownlineSizes = 0;
    _.each(self.users, (u) => {
      u.newDownlineSize = 0;
      u.newDownlineUsers = {};
    });
    _.each(self.users, (u) => {
      let upline: any[] = self.uplineUsers(u);
      u.newDownlineLevel = upline.length + 1;
      if (upline[0]) {
        upline[0].newDownlineUsers[u.userId] = _.pick(u, ['name', 'profilePhotoUrl']);
      }
      _.each(upline, (uplineUser) => { uplineUser.newDownlineSize++; });
    });
    _.each(self.users, (u) => {
      if (u.newDownlineLevel !== u.downlineLevel) {
        u.downlineLevel = u.newDownlineLevel;
        self.userRef(u).update({downlineLevel: u.downlineLevel});
        updatedDownlineLevels++;
      }

      if (!_.isEqual(u.newDownlineUsers, u.downlineUsers || {})) {
        u.downlineUsers = u.newDownlineUsers;
        self.userRef(u).update({downlineUsers: u.downlineUsers});
        updatedDownlineUsers++;
      }

      if (u.newDownlineSize !== u.downlineSize) {
        u.downlineSize = u.newDownlineSize;
        self.userRef(u).update({downlineSize: u.downlineSize});
        updatedDownlineSizes++;
      }
    });

    log.info(`updatedDownlineLevels=${updatedDownlineLevels}`);
    log.info(`updatedDownlineUsers=${updatedDownlineUsers}`);
    log.info(`updatedDownlineSizes=${updatedDownlineSizes}`);
  }

  unblockUsers() {
    let self = this;
    let reassigned = 0;

    let blockedUsers: any[] = _.filter(this.users, (u) => { return this.userState(u) === 'blocked-by-upline' && _.includes(['missing-wallet','disabled'], this.sponsorState(u)) });
    let numBlocked = _.size(blockedUsers);

    _.each(blockedUsers, (u, uid) => {
      let newSponsor = _.find(self.uplineUsers(self.getSponsor(u)), (uplineUser, index) => {
        return index > 0 && _.includes([
          'announcement-confirmed', 'waiting-for-announcement-confirmation', 'waiting-for-review',
          'waiting-for-docs',
          'blocked-by-upline', 'waiting-for-upline-processing', 'waiting-for-announcement-to-be-queued'
        ], this.userState(uplineUser));

      });
      if (newSponsor) {
        u.oldSponsor = _.merge(_.clone(u.sponsor), { replacedAt: firebase.database.ServerValue.TIMESTAMP });
        u.newSponsor = _.pick(newSponsor, ['userId', 'name', 'profilePhotoUrl']);
        u.newSponsor.announcementTransactionConfirmed = !!newSponsor.wallet &&
            !!newSponsor.wallet.announcementTransaction &&
            !!newSponsor.wallet.announcementTransaction.blockNumber &&
            !!newSponsor.wallet.announcementTransaction.hash;
      }
    });

    _.each(self.users, (u, uid) => {
      if (u.newSponsor) {
        u.sponsor = u.newSponsor;
        self.userRef(u).update({ oldSponsor: u.oldSponsor, sponsor: u.sponsor}).then(() => {
          reassigned++;
          log.info(`reassigned record ${reassigned}`);
          log.info(`u.userId: ${u.userId}, u.oldSponsor.userId: ${u.oldSponsor.userId}, u.newSponsor.userId: ${u.newSponsor.userId}`);
        });
      }
    });

    self.fixDownlineInfo();

    log.info(`${numBlocked} blocked users`);
    log.info(`${reassigned} blocked users were reassigned`);
  }

  private referralUsers(u: any): any[] {
    return _.filter(this.users, (r: any) => {
      return r.sponsor && r.sponsor.userId === u.userId && r.userId !== u.userId
    });
  }

  private downlineUsers(u: any): any[] {
    if (!u.calculatedDownlineUsers) {
      let referrals: any[] = this.referralUsers(u);
      u.calculatedDownlineUsers = referrals.concat(_.flatten(_.map(referrals, (r) => { return this.downlineUsers(r); })));
    }
    return u.calculatedDownlineUsers;
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
      let tasksRef = self.ref(`/phoneAuthQueue/tasks`);
      tasksRef.once('value', (snapshot: firebase.database.DataSnapshot) => {
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

  announceUsers() {
    this.ref(`/identityAnnouncementQueue/tasks`).once('value', (snapshot: firebase.database.DataSnapshot) => {
      let identityAnnouncementTasks: any = snapshot.val() || {}
      // _.each(identityAnnouncementTasks, (t, tid) => { t.taskId = tid; });
      // log.info('grouped by state', _.groupBy(identityAnnouncementTasks, '_state'));
      // log.info('grouped by error', _.groupBy(identityAnnouncementTasks, '_error_details.error'));

      let alreadyAnnouncedUserIds: any[] = _.map(_.reject(identityAnnouncementTasks, '_state'), 'userId');
      log.info(`count of alreadyAnnouncedUserIds: ${_.size(alreadyAnnouncedUserIds)}`);
      log.info(`alreadyAnnouncedUserIds: ${alreadyAnnouncedUserIds}`);

      let announceableUsers: any[] = _.filter(this.users, (u: any) => {
        return this.userState(u) === 'waiting-for-announcement-to-be-queued' && !_.includes(alreadyAnnouncedUserIds, u.userId);
      });

      _.each(announceableUsers, (u: any) => {
        log.info(`url=${this.userRef(u).toString()}`);
        this.ref('/walletCreatedQueue/tasks').push({ userId: u.userId });
      });
      log.info(`number of users announced: ${_.size(announceableUsers)}`);
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
    let groups: any;

    groups = _.groupBy(this.users, 'state');
    _.each(groups, (group, state) => {
      log.info(`  state: ${state}, count: ${_.size(group)}`);
    });

    let blockedUsers: any[], rootUsers: any[];

    blockedUsers = groups['blocked-by-upline'];
    rootUsers = _.uniq(_.map(blockedUsers, (u) => { return this.rootUser(u); }));
    rootUsers = _.sortBy(rootUsers, (u) => { u.state = this.userState(u); return u.state });
    log.info(`\nblocking users: ${_.size(rootUsers)}, number blocked: ${_.size(blockedUsers)}`);
    _.each(rootUsers, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state}`); });

    blockedUsers = _.filter(blockedUsers, (u) => { return _.includes(['missing-wallet','disabled'], this.sponsorState(u)) });
    log.info(`\nblocked users who should be moved: ${_.size(blockedUsers)}`);
    _.each(blockedUsers, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state}`); });

    blockedUsers = groups['waiting-for-upline-processing'];
    rootUsers = _.uniq(_.map(blockedUsers, (u) => { return this.rootUser(u); }));
    rootUsers = _.sortBy(rootUsers, 'state');
    log.info(`\nkey users needing review or other processing: ${_.size(rootUsers)}, number blocked: ${_.size(blockedUsers)}`);
    _.each(rootUsers, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state}`); });

    blockedUsers = groups['fraud-suspected'];
    log.info(`\nusers suspected of fraud: ${_.size(blockedUsers)}`);
    _.each(blockedUsers, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state}`); });

    // let confirmedUsers: any[] = _.filter(this.users, {state: 'announcement-confirmed'});
    // let millisecondsPerDay = 1000 * 60 * 60 * 24;
    // let usersLeft = _.size(confirmedUsers)
    // _.each(confirmedUsers, (u) => {
    //   let hash: number = u.wallet.announcementTransaction.hash;
    //   this.userRef(u).child(`transactions/${hash}/createdAt`).once('value', (snapshot: firebase.database.DataSnapshot) => {
    //     let x: number = parseInt(snapshot.val());
    //     u.dayCreated = x - ( x % millisecondsPerDay );
    //     usersLeft--;
    //     if (usersLeft % 1000 === 0) {
    //       log.info(`processed 2000 users`);
    //     }
    //     if (usersLeft === 0) {
    //       let groups = _.groupBy(confirmedUsers, 'dayCreated');
    //       _.each(groups, (g: any[], dayCreated: string) => {
    //         let minBlockNumber: any = _.min(_.map(g, 'wallet.announcementTransaction.blockNumber'));
    //         log.info(`${dayCreated} ;${dayCreated ? new Date(parseInt(dayCreated)) : 'none'};${minBlockNumber};${_.size(g)}`);
    //       });
    //     }
    //   });
    // });

  }


  openMissingFreshdeskTickets() {
    let usersWithReviewPending: any[] = _.filter(this.users, (u) => { return this.userState(u) === 'waiting-for-review'; });
    usersWithReviewPending = _.reject(usersWithReviewPending, 'freshdeskUrl');

    log.info(`about to open ${_.size(usersWithReviewPending)} tickets`);
    // _.each(usersWithReviewPending, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state} / ${u.downlineSize} / ${u.freshdeskUrl}`); });

    // let manualIDVerifier: ManualIDVerifier = new ManualIDVerifier( this.env, this.db );
    // _.each(usersWithReviewPending, (u: any) => {
    //   manualIDVerifier.openFreshdeskTicketIfNecessary(u.userId);
    // });
  }

  checkTransactions() {
    let potentiallyInvalidTransactions: any[] = require('../data/potentiallyInvalidTransactions.json');

    let Web3 = require('web3');
    let web3 = new Web3(new Web3.providers.HttpProvider('http://127.0.0.1:9595'));

    log.info(`about to check ${_.size(potentiallyInvalidTransactions)} potentially invalid hashes`);

    let chunk = _.slice(potentiallyInvalidTransactions, 0, 5000);
    let numLeft = _.size(chunk);
    _.each(chunk, (p, index) => {
      let ref = this.userRef({userId: p.userId}).child(`transactions/${p.hash}`);
      log.info(`about to load fb transaction data at ${ref.toString()}`);
      ref.once('value', (snapshot: firebase.database.DataSnapshot) => {
        numLeft--;
        if (snapshot.exists()) {
          log.info(`found fb transaction data at ${ref.toString()}`);

          log.info(`about to retrieve network transaction data ${p.hash}`);

          let retrievedTransaction: any = web3.eth.getTransaction(p.hash);
          if (retrievedTransaction) {
            log.info(`network transaction data found -- skipping`);
          } else {
            log.info(`network transaction data NOT found`);
            log.info(`removing fb transaction data`);
            ref.remove();
            log.info(`removed fb transaction data at ${ref.toString()}`);

            ref = this.userRef({userId: p.userId}).child(`events/${p.hash}`);
            ref.remove();
            log.info(`removed fb event at ${ref.toString()}`);
          }
        }
        if (numLeft === 0) {
          log.info('did them all');
        }
      });
    });
  }

  disableFraudulentUser(email: string) {
    let matchedUsers: any[] = _.filter(this.users, {email: email});

    let count = _.size(matchedUsers);
    if (count === 0 || count > 1) {
      log.info(`found ${count} users with email ${email}`);
      return;
    }

    this.userRef(matchedUsers[0]).update({disabled: true, fraudSuspected: true}).then(() => {
      log.info(`successfully disabled user with email ${email}`);
    });
  }

}
