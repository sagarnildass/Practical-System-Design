'use client';

import { useState, useEffect } from 'react';
import { Post } from '@/lib/types';
import { getUserFeed } from '@/lib/api';
import { PostCard } from '@/components/post-card';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';

interface UserFeedProps {
  userId: string;
}

export function UserFeed({ userId }: UserFeedProps) {
  const [feed, setFeed] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processedFeed, setProcessedFeed] = useState<Post[]>([]);

  const fetchFeed = async () => {
    if (!userId) return;

    try {
      setLoading(true);
      const feedData = await getUserFeed(userId);
      setFeed(feedData.posts);
      console.log("Original feed:", feedData.posts);
    } catch (err) {
      setError('Failed to load feed');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // Process feed to avoid showing shared posts and their originals at the same level
  // and filter out comments that are displayed as posts
  useEffect(() => {
    if (feed.length === 0) return;

    // Find all parent post IDs from shared posts and comments
    const parentPostIds = new Set<string>();
    feed.forEach(post => {
      if (post.parent_post_id) {
        parentPostIds.add(post.parent_post_id);
      }
    });

    console.log("Parent post IDs:", Array.from(parentPostIds));

    // Create a map to group related posts by their ID
    const postsMap = new Map<string, Post>();
    feed.forEach(post => {
      postsMap.set(post.post_id, post);
    });

    // Filter the feed to only show top-level posts
    // A post is top-level if:
    // 1. It's not a comment (post_type is not 'comment')
    // 2. It's either a regular post or a share
    // 3. If it's being shared by another post in the feed, we show the share instead
    const filtered = feed.filter(post => {
      // Check if this post has a parent (meaning it's a comment or share)
      const hasParent = !!post.parent_post_id;
      
      // Normalize post type to lowercase for case-insensitive comparison
      const postType = post.post_type.toLowerCase();
      
      // Check if this post is being shared by another post in the feed
      const isBeingShared = parentPostIds.has(post.post_id) && 
        Array.from(postsMap.values()).some(p => 
          p.parent_post_id === post.post_id && 
          p.post_type.toLowerCase() === 'share'
        );
      
      // Keep the post if:
      // 1. It's not a comment
      // 2. It's not already being shown as a shared content in another post
      return postType !== 'comment' && !isBeingShared;
    });

    // Sort by creation date, newest first
    filtered.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

    console.log("Processed feed:", filtered);
    setProcessedFeed(filtered);
  }, [feed]);

  useEffect(() => {
    fetchFeed();
  }, [userId]);

  if (loading) {
    return (
      <div className="space-y-4">
        {[1, 2, 3].map((i) => (
          <div key={i} className="rounded-lg border p-4">
            <div className="flex items-center space-x-3">
              <Skeleton className="h-10 w-10 rounded-full" />
              <div className="space-y-2">
                <Skeleton className="h-4 w-[200px]" />
                <Skeleton className="h-3 w-[150px]" />
              </div>
            </div>
            <div className="mt-4 space-y-2">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border bg-red-50 p-4 text-red-500">
        Error: {error}
        <Button 
          onClick={fetchFeed} 
          variant="outline" 
          size="sm" 
          className="ml-2"
        >
          Retry
        </Button>
      </div>
    );
  }

  if (processedFeed.length === 0) {
    return (
      <div className="rounded-lg border bg-gray-50 p-8 text-center">
        <p className="text-gray-500">No posts in your feed yet.</p>
        <p className="text-sm text-gray-400">Follow more users to see their posts here.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between">
        <h2 className="text-xl font-bold">Your Feed</h2>
        <Button onClick={fetchFeed} variant="outline" size="sm">
          Refresh
        </Button>
      </div>
      <div className="space-y-4">
        {processedFeed.map((post) => (
          <PostCard 
            key={post.post_id} 
            post={post} 
            currentUserId={userId} 
            onRefresh={fetchFeed}
          />
        ))}
      </div>
    </div>
  );
} 