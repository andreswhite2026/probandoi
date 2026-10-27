import { createRouter, createWebHistory } from 'vue-router'
import { useAuth } from '../services/auth'
import LoginView from '../views/LoginView.vue'
import SeismicMonitorView from '../views/SeismicMonitorView.vue'
import ReportsAiView from '../views/ReportsAiView.vue'
import OperationsView from '../views/OperationsView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/login',
      name: 'login',
      component: LoginView,
      meta: { guestOnly: true }
    },
    {
      path: '/',
      redirect: '/monitor'
    },
    {
      path: '/monitor',
      name: 'monitor',
      component: SeismicMonitorView,
      meta: { requiresAuth: true }
    },
    {
      path: '/reports',
      name: 'reports',
      component: ReportsAiView,
      meta: { requiresAuth: true }
    },
    {
      path: '/operations',
      name: 'operations',
      component: OperationsView,
      meta: { requiresAuth: true }
    },
    {
      path: '/:pathMatch(.*)*',
      redirect: '/monitor'
    }
  ]
})

router.beforeEach((to, _from, next) => {
  const { isAuthenticated } = useAuth()

  if (to.meta.requiresAuth && !isAuthenticated.value) {
    next({ name: 'login', query: { redirect: to.fullPath } })
  } else if (to.meta.guestOnly && isAuthenticated.value) {
    next({ name: 'monitor' })
  } else {
    next()
  }
})

export default router
