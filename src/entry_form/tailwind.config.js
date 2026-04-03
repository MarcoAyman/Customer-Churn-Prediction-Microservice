/** @type {import('tailwindcss').Config} */
export default {
  /* Tell Tailwind which files to scan for class names.
     Only classes found here are included in the final CSS bundle. */
  content: [
    './index.html',
    './src/**/*.{js,jsx}',
  ],
  theme: {
    extend: {
      /* Custom font families that match the glassmorphism aesthetic */
      fontFamily: {
        display: ['Syne', 'sans-serif'],
        body:    ['Inter', 'sans-serif'],
        mono:    ['JetBrains Mono', 'monospace'],
      },
      /* Custom colours used throughout the form.
         These extend Tailwind's default palette — use them as:
         bg-glass-surface, text-teal-accent, etc. */
      colors: {
        glass: {
          base:    '#090d14',
          surface: 'rgba(255,255,255,0.04)',
          card:    'rgba(255,255,255,0.055)',
          border:  'rgba(255,255,255,0.11)',
          strong:  'rgba(255,255,255,0.18)',
        },
        teal: {
          accent:  '#14b8a6',
          dim:     'rgba(20,184,166,0.18)',
          border:  'rgba(20,184,166,0.45)',
          glow:    'rgba(20,184,166,0.07)',
          text:    'rgba(20,184,166,0.95)',
        },
        indigo: {
          dim:     'rgba(99,102,241,0.15)',
          border:  'rgba(99,102,241,0.30)',
          text:    'rgba(165,163,255,0.9)',
        },
      },
      /* Border radius values that match the glass panel design */
      borderRadius: {
        'glass-outer': '24px',
        'glass-inner': '18px',
        'glass-field': '10px',
        'glass-btn':   '11px',
      },
      /* Keyframe animations used in the form */
      keyframes: {
        fadeUp: {
          '0%':   { opacity: '0', transform: 'translateY(6px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        shimmer: {
          '0%':   { opacity: '0.4' },
          '50%':  { opacity: '1' },
          '100%': { opacity: '0.4' },
        },
        spin: {
          '0%':   { transform: 'rotate(0deg)' },
          '100%': { transform: 'rotate(360deg)' },
        }
      },
      animation: {
        'fade-up': 'fadeUp 0.35s ease-out forwards',
        'shimmer': 'shimmer 1.5s ease-in-out infinite',
        'spin':    'spin 0.8s linear infinite',
      }
    },
  },
  plugins: [],
}
