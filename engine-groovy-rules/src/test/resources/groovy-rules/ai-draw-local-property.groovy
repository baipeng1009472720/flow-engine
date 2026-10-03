// 用途：校验每日次数并按累计序号执行可复现九宫格抽奖
// 输入：state.drawsToday、state.lastDrawDate、state.totalDraws、state.lastResult、state.lastRewardId、today
// 输出：成功时更新 state 并返回 viewModel/effect；超限时保持 state 原样
 def state = ctx.entry.request.state
 def drawsToday = state.drawsToday ?: 0
 def lastDate = state.lastDrawDate ?: ''
 def currentDraws = lastDate == ctx.entry.request.today ? drawsToday : 0
 def totalDraws = state.totalDraws ?: 0
 // 达到每日上限时直接返回，保证状态和次数都不发生变化
 if (currentDraws >= 3) {
   return [state: state, viewModel: [drawsToday: currentDraws, remainingDraws: 0, dailyLimit: 3, lastResult: state.lastResult ?: '', lastRewardId: state.lastRewardId ?: '', canDraw: false, message: '今日抽奖次数已用完'], effect: [:]]
 }
 def nextTotalDraws = totalDraws + 1
 def bucket = nextTotalDraws % 10
 // 用累计序号桶位替代真随机，保证测试与运行结果可复现
 def rewardId = bucket == 1 ? '2081386715758604290' : (bucket == 2 ? '2081386577749225474' : '')
 def result = bucket == 1 ? '联调发放积分1785076237' : (bucket == 2 ? '联调积分1785076204' : '谢谢参与')
 def nextDraws = currentDraws + 1
 def nextRemaining = 3 - nextDraws
 def nextState = [drawsToday: nextDraws, lastDrawDate: ctx.entry.request.today, totalDraws: nextTotalDraws, lastResult: result, lastRewardId: rewardId]
 def nextEffect = rewardId == '' ? [:] : [rewardId: rewardId]
 return [state: nextState, viewModel: [drawsToday: nextDraws, remainingDraws: nextRemaining, dailyLimit: 3, lastResult: result, lastRewardId: rewardId, canDraw: nextRemaining > 0, message: ''], effect: nextEffect]