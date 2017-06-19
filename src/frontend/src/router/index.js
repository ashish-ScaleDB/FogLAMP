import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router)

const router = new Router({
  mode: 'history',
  routes: [
    // Each of these routes are loaded asynchronously, when a user first navigates to each corresponding endpoint.
    // The route will load once into memory, the first time it's called, and no more on future calls.
    // This behavior can be observed on the network tab of your browser dev tools.
    {
      path: '/login',
      name: 'login',
      component: function (resolve) {
        require(['@/components/Login.vue'], resolve)
      }
    },
    {
      path: '/signup',
      name: 'signup',
      component: function (resolve) {
        require(['@/components/Signup.vue'], resolve)
      }
    },
    {
      path: '/',
      name: 'dashboard',
      component: function (resolve) {
        require(['@/components/Dashboard.vue'], resolve)
      },
      // beforeEnter: guardRoute
    }
  ]
})

export default router
