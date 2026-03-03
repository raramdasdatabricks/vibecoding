'use strict';

// ── Nav scroll effect ──────────────────────────────────────────────────────
const nav = document.getElementById('nav');

const onScroll = () => {
  nav.classList.toggle('scrolled', window.scrollY > 20);
};
window.addEventListener('scroll', onScroll, { passive: true });
onScroll();

// ── Mobile menu ────────────────────────────────────────────────────────────
const hamburger = document.getElementById('hamburger');
hamburger.addEventListener('click', () => {
  nav.classList.toggle('menu-open');
  const open = nav.classList.contains('menu-open');
  hamburger.setAttribute('aria-expanded', open);
});

// Close menu when a nav link is clicked
document.querySelectorAll('.nav__links a').forEach(link => {
  link.addEventListener('click', () => nav.classList.remove('menu-open'));
});

// ── Reveal on scroll ───────────────────────────────────────────────────────
const revealObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry, i) => {
      if (entry.isIntersecting) {
        // Stagger cards in the same parent
        const siblings = entry.target.parentElement.querySelectorAll('.reveal:not(.visible)');
        let delay = 0;
        siblings.forEach(el => {
          if (el === entry.target || isInViewport(el)) {
            el.style.transitionDelay = `${delay}ms`;
            el.classList.add('visible');
            delay += 80;
          }
        });
        entry.target.classList.add('visible');
        revealObserver.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.12, rootMargin: '0px 0px -40px 0px' }
);

function isInViewport(el) {
  const rect = el.getBoundingClientRect();
  return rect.top < window.innerHeight && rect.bottom > 0;
}

document.querySelectorAll('.reveal').forEach(el => revealObserver.observe(el));

// ── Contact form ───────────────────────────────────────────────────────────
const form       = document.getElementById('contactForm');
const formSuccess = document.getElementById('formSuccess');
const submitBtn  = document.getElementById('submitBtn');

if (form) {
  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    if (!validateForm()) return;

    // Loading state
    submitBtn.classList.add('btn--loading');
    submitBtn.disabled = true;

    // Simulate async submission (replace with real endpoint)
    await fakeSubmit();

    form.hidden = true;
    formSuccess.hidden = false;
  });
}

function validateForm() {
  let valid = true;
  const required = form.querySelectorAll('[required]');

  required.forEach(field => {
    field.classList.remove('error');
    const val = field.value.trim();

    if (!val) {
      field.classList.add('error');
      valid = false;
    } else if (field.type === 'email' && !isValidEmail(val)) {
      field.classList.add('error');
      valid = false;
    }
  });

  if (!valid) {
    const firstError = form.querySelector('.error');
    firstError?.focus();
    firstError?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
  return valid;
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function fakeSubmit() {
  return new Promise(resolve => setTimeout(resolve, 1400));
}

// Clear error state on input
form?.querySelectorAll('input, select, textarea').forEach(field => {
  field.addEventListener('input', () => field.classList.remove('error'));
});

// ── Smooth anchor scroll ───────────────────────────────────────────────────
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
  anchor.addEventListener('click', (e) => {
    const target = document.querySelector(anchor.getAttribute('href'));
    if (!target) return;
    e.preventDefault();
    const offset = 80; // nav height
    const top = target.getBoundingClientRect().top + window.scrollY - offset;
    window.scrollTo({ top, behavior: 'smooth' });
  });
});

// ── Active nav link highlight ──────────────────────────────────────────────
const sections = document.querySelectorAll('section[id]');
const navLinks  = document.querySelectorAll('.nav__links a');

const sectionObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        navLinks.forEach(link => {
          link.style.color = link.getAttribute('href') === `#${entry.target.id}`
            ? 'var(--text)'
            : '';
        });
      }
    });
  },
  { rootMargin: '-40% 0px -50% 0px' }
);

sections.forEach(s => sectionObserver.observe(s));
